library(tidyr)

# Function to weight weekdays differently
weekend_weights <- function(triangle, delay = 0, weekend_weight = 0) { 
  
  weight <- 1 - weekend_weight
 
  # Check for reporting days which are weekends 
  weekdays <- lubridate::wday(as_date(dimnames(triangle)$event_date) + delay, label = TRUE)
  weekends <- which(weekdays %in% c("Sat", "Sun"))
  
  calPeriods <- (row(triangle) + col(triangle) - 1)
  
  # Initalize weights 
  weight_mat <- triangle
  weight_mat[!is.na(triangle)] <- weight
  
  # Adjust for weekends 
  for (i in seq_along(weekends)) { 
    rep_mat <- if_else(calPeriods == weekends[[i]], TRUE, FALSE)
    weight_mat[rep_mat] <- weekend_weight
  }
  weight_mat
}


nowcast_summary_poisson <- function(reporting_date, cc_long, stan, weekend_weight = 0, lag = 1, max_delay = 30, beta.priors = rep(0.1, times=max_D)) { 
  
  reporting_date <- as_date(reporting_date)
  
  itr_ultimate <- cc_long |>
    cumulative_to_incremental() |>
    to_triangle_matrix(date_newest_event = reporting_date, observation_window = 2 *max_delay, 
                       max_delay = max_delay, 
                       lag = lag)
  
  # Reconstruct trapezoid at T = reporting_date based on historical data
  itr <- itr_ultimate |>
    wipe_trapezoid()
  
  # Prepare incremental count for stan
  itr[is.na(itr)] <- 0 # Replace NA: Can not be handled by stan algorithm 
  itr[itr < 0] <- 0 # Negative values (corrections) are set to zero (poisson > 0)
  
  # weekend_weight = 0: Ignore data, 1 = keep data
  Z <- weekend_weights(itr, weekend_weight = weekend_weight)
  Z[is.na(Z)] <- 0  # (t + d > T will not be considered for fitting)
  
  max_D <- ncol(itr) 
  beta.priors <- beta.priors
  
  model.data <- list(
    rt = itr,
    T = nrow(itr),
    D = max_D, 
    Z = Z,
    omega = beta.priors
  )
  
  # Run MCMC algorithm 
  samples <- model$sample(data = model.data, 
                          refresh = 0,
                          seed = 4142,
                          chains = 4,
                          parallel_chains = 4,
  )  
  
  # Get draws for N
  mcmc_out <- samples$draws("N") %>% 
    as_tibble() %>%
    tidyr::gather(key, value) %>%
    mutate(key = stringr::str_split(key, "\\.", simplify = T)[, 2]) %>%
    tidyr::pivot_wider(names_from = key, values_from = value, values_fn = list) %>%
    purrr::flatten() %>%
    as_tibble() %>%
    # TODO: Double check trapezoid logic --> Only keep predictions for triangle not full trapezoid
    select((ncol(.) - max_D + 1):ncol(.))
  
  # TODO: Check and fix if this is correct (extract triangle from trapezoid)
  idx_tr <- (nrow(itr_ultimate) - max_delay + 1):nrow(itr_ultimate)
  itr <- itr[idx_tr, ]
  itr_ultimate <- itr_ultimate[idx_tr, ]
  
  summary <- tibble(
    reporting_date = reporting_date + lag,
    week_day = lubridate::wday(reporting_date, label = TRUE),
    event_date = rownames(itr),
    delay = nrow(itr):(nrow(itr)-length(event_date) + 1),
    newest_reported = apply(itr, 1, sum), # Cumulative count at reporting day - lag
    ultimate = apply(itr_ultimate, 1, sum), # Cumulative count after max delay
    ibnr = ultimate - newest_reported,
    predict = NA, # Not computed 
    sd = NA, # Not computed
    predicted_median = as.integer(apply(mcmc_out, 2, median)),
    predicted_q5 = as.integer(apply(mcmc_out, 2, function(x) quantile(x, .025))),
    predicted_q95 = as.integer(apply(mcmc_out, 2, function(x) quantile(x, .975))), 
    se_p2 = (ultimate - predicted_median) ^ 2 # Squared deviation prediction vs ultiamte observed count
  ) 
  
}

# Return nowcast for a given reporting_date  
nowcast_summary_mack <- function(reporting_date, cc_long,   
                                 lag = 1, max_delay = 30, observation_window = 2 * max_delay, ...) { 
  
  cat("Nowcasting: ", reporting_date, "\n")
  
  reporting_date <- as_date(reporting_date)
  
  # Reporting triangle where T = reporting date  
  ctr_ultimate <- cc_long |>
    to_triangle_matrix(date_newest_event = reporting_date, observation_window = observation_window, 
                       max_delay = max_delay, 
                       lag = lag)
  
  # Extract triangle from trapezoid for summary
  idx_tr <- (nrow(ctr_ultimate) - max_delay + 1):nrow(ctr_ultimate)
  ctt <- ctr_ultimate[idx_tr, ]
  
  # Reconstruct trapezoid at T = reporting_date based on historical data
  ctr <- ctr_ultimate |>
    wipe_trapezoid()
  
  # nowcast: On trapezoid --> Return triangle
  nowcast <- mack_chain_ladder(ctr)
  
  summary <- tibble(
    reporting_date = reporting_date + lag,
    week_day = lubridate::wday(reporting_date, label = TRUE),
    event_date = as_date(rownames(ctt)),
    delay = nrow(ctt):(nrow(ctt)-length(event_date) + 1),
    newest_reported = data_by_event_index(ctt),
    ultimate = ctt[, max_delay], 
    ibnr = ultimate - newest_reported,
    predict = nowcast$predict[idx_tr],
    sd = nowcast$s.e[idx_tr],
    predicted_median = NA,
    predicted_q5 = NA,
    predicted_q95 = NA, 
    se_p2 = round((ultimate - predict) ^ 2, 2)
  )
}


## Function to nowcast over observation window ----
nowcast <- function(events, nowcast_function, ...) {
  
  cat("Nowcasting with latest reporting day: ", as.character(max(events$event_date) + min(events$delay)), "\n")
  
  reporting_triangle <-  to_triangle_matrix(events)
  
  weight_matrix <- to_weight_matrix(events)
  
  prediction <- nowcast_function(reporting_triangle, weight_matrix, ...)
  
}

## Nowcast function implementations ----
mack_chain_ladder <- function(reporting_triangle, 
                              weights, 
                              alpha, 
                              est.sigma = "Mack") {
  
  reporting_date_latest <- as_date(rownames(reporting_triangle)[nrow(reporting_triangle)]) + as.integer(colnames(reporting_triangle)[1])
  cat("Nowcasting with latest reporting date: ", as.character(reporting_date_latest), "\n")
  
  # Remark: Reporting triangle needs to be cumulative 
  mack <- suppressWarnings(ChainLadder::MackChainLadder(reporting_triangle, 
                                                        weights = weights, 
                                                        alpha = alpha, 
                                                        est.sigma = est.sigma))
  
}

summary_observation_frame_chain_ladder <- function(nowcast_result, ultimate_counts) { 
  
  # Remark: Only consider the predicted triangle, not the full observation trapezoid
  
  min_delay <- as.integer(colnames(nowcast_result$Triangle)[[1]])
  max_delay <- as.integer(colnames(nowcast_result$Triangle)[[ncol(nowcast_result$Triangle)]])
  max_event_date <- as_date(rownames(nowcast_result$Triangle)[nrow(nowcast_result$Triangle)])
  
  # Reporting data 
  reported_data <- nowcast_result$Triangle |>
    from_triangle_matrix() |>
    filter(reporting_date == max(reporting_date)) |>
    rename(reporting_delay = delay, 
           reported_entries = entries) |>
    select(-reporting_date)
    
  ultimate_data <- ultimate_counts |>
    filter(event_date %in% reported_data$event_date,
           delay == max_delay) |>
    rename(ultimate_delay = delay,
           ultimate_entries = entries) |>
    select(-reporting_date)
  
  # Only predict for the missing lower triangle not full trapezoid
  
  predicted_data <- from_triangle_matrix(nowcast_result$FullTriangle) |>
    filter(delay == max_delay) |>
    rename(prediction_delay = delay,
           predicted_entries = entries) |>
    select(-reporting_date)
  
  s.e <- from_triangle_matrix(nowcast_result$Mack.S.E, values_drop_na = FALSE) |>
    filter(delay == max_delay) |>
    rename(s.e = entries) |>
    select(-reporting_date, -delay) 
  
  # Confidence interval: Based on log-normal distribution
  #sigma_i_2 <- log(1 + (s.e$s.e)^2 / prediction$predicted_entries^2)
  #mu_i <- log(prediction$predicted_entries) - sigma_i_2 / 2
  #jlb <- round(exp(mu_i + qnorm(0.025)*sqrt(sigma_i_2)), 2)
  #ub <- round(exp(mu_i + qnorm(0.975)*sqrt(sigma_i_2)), 2)
  
  observation_frame_summary <- reduce(list(reported_data, ultimate_data, predicted_data, s.e), left_join, by = c("event_date")) |>
    mutate(sigma_i_2 = log(1 + (s.e)^2 / predicted_entries^2),
           mu_i = log(predicted_entries) - sigma_i_2 / 2,
           lb = floor(exp(mu_i + qnorm(0.025)*sqrt(sigma_i_2))),
           ub = ceiling(exp(mu_i + qnorm(0.975)*sqrt(sigma_i_2))),
           interval_type = "quantile",
           se = (ultimate_entries - predicted_entries)^2, # Squared error
           squared_relative_error = ((ultimate_entries - predicted_entries)/ ultimate_entries) ^ 2,
           pi_coverage = between(ultimate_entries, lb, ub)) |>
    select(-s.e, -sigma_i_2, -mu_i)
  
}

set_non_reported_to_na <- function(reporting_summary) { 
  
  tidy <- reporting_summary |> 
    mutate(across(c(reporting_delay, reported_entries, ultimate_delay, ultimate_entries, predicted_entries, prediction_delay, lb, ub, interval_type, rmse, rmsre), 
                  ~ case_when(wday(reporting_date, label = TRUE) %in% c("Sat", "Sun") ~ NA, 
                              TRUE ~ .x)))
}

# Bayesian nowcasting ----
bayesian_nowcast <- function(reporting_triangle, 
                             stan_model, 
                             weights, 
                             theta, 
                             normal, 
                             gamma) {
  
  min_delay <- as.integer(colnames(reporting_triangle)[1])
  max_delay <- as.integer(colnames(reporting_triangle)[ncol(reporting_triangle)])
  reporting_date_latest <- as_date(rownames(reporting_triangle)[nrow(reporting_triangle)]) + as.integer(colnames(reporting_triangle)[1])
  
  cat("Nowcasting with latest reporting date: ", as.character(reporting_date_latest), "\n")
  
  # Stan model parameters
  reporting_triangle[is.na(reporting_triangle)] <- 0 # Replace NA: Can not be handled by stan algorithm 
  reporting_triangle[reporting_triangle < 0] <- 0 # Negative values (corrections) are set to zero (poisson > 0)
  
  # weekend_weight = 0: Ignore data, 1 = keep data
  weights <- weights
  weights[is.na(weights)] <- 0  # (t + d > T will not be considered for fitting)
  

  data <- list(
    rt = reporting_triangle,
    T = nrow(reporting_triangle),
    D = ncol(reporting_triangle), 
    Z = weights,
    theta = theta,
    normal = normal, 
    gamma = gamma  
  )
  
  # Run MCMC algorithm 
  stan_fit <- rstan::sampling(stan_model,
                             data = data,
                             chains = 4, 
                             cores = 4,
                             verbose = FALSE)
  
  results <- list(stan_fit = stan_fit, 
                  data = data)
  
}

summary_observation_frame_bayesian_nowcast <- function(nowcast_result, ultimate_counts) { 
  
  stan_fit <- nowcast_result$stan_fit
  data <- nowcast_result$data
  
  min_delay <- as.integer(colnames(data$rt)[[1]])
  max_delay <- as.integer(colnames(data$rt)[[ncol(data$rt)]])
  max_event_date <- as_date(rownames(data$rt)[nrow(data$rt)])
  
  # Get draws for N 
  #mcmc_out <- samples$draws("N") %>% 
  #  as_tibble() %>%
  #  tidyr::gather(key, value) %>%
  #  mutate(key = stringr::str_split(key, "\\.", simplify = T)[, 2]) %>%
  #  tidyr::pivot_wider(names_from = key, values_from = value, values_fn = list) %>%
  #  purrr::flatten() %>%
  #  as_tibble() |>
  #  setNames(rownames(model_data$rt))
  
  mcmc_out <-  tidybayes::tidy_draws(stan_fit) |>
    select(.chain, .iteration, .draw, starts_with("N", ignore.case = FALSE)) %>%
    gather(key = key, value = value, starts_with("N")) %>%
    mutate(key = gsub("N|\\[|\\]", "", key) %>% as.integer())
  
  mcmc_stats <- mcmc_out |>
    group_by(key) |>
    summarise(median = median(value), 
              lb = quantile(value, 0.025, na.rm = TRUE), 
              ub = quantile(value, 0.975, na.rm = TRUE)
    )
  
  # Get median statistics
  prediction <- tibble(
    event_date = as_date(rownames(data$rt)),
    prediction_delay = as.integer(colnames(data$rt)[ncol(data$rt)]),
    predicted_entries = mcmc_stats$median,
    lb = mcmc_stats$lb,
    ub = mcmc_stats$ub
  )
  
  # Reporting data 
  reported <- tibble(event_date = as_date(rownames(data$rt)), 
                     reported_entries = rowSums(data$rt))
    
  published <- ultimate_counts |>
    select(-reporting_date) |>
    mutate(delay = as.integer(delay), 
           reporting_delay = min_delay, 
           ultimate_delay = max_delay) |>
    rename(reported_entries = entries) |>
    filter(delay %in% c(min_delay, max_delay)) |>
    pivot_wider(names_from = delay, values_from = reported_entries) |>
    rename( ultimate_entries = as.character(max_delay ))
  
  # Summary over complete observation_frame
  observation_frame_summary <- reduce(list(reported, published, prediction), left_join, by = c("event_date")) |>
    mutate(interval_type = "quantile", 
           se = (ultimate_entries - predicted_entries)^2, # Prediction error
           squared_relative_error = ((ultimate_entries - predicted_entries)/ ultimate_entries) ^ 2,
           pi_coverage = between(ultimate_entries, lb, ub))
  
}

summary_last_event_aggregation <- function(summary_observation_frame, rmse_window = 7, min_delay = 1) {
  
  # Summary for most recent event_date on publication date 
  last_event_day_summary <- summary_observation_frame |>
    filter(reporting_delay >= min_delay) |>
    arrange(event_date) |>
    slice_tail(n = 7) |>
    summarise(event_date = last(event_date),
              reporting_delay = last(reporting_delay), 
              reported_entries = last(reported_entries),
              prediction_delay = last(prediction_delay), 
              predicted_entries = last(predicted_entries), 
              ultimate_delay = last(ultimate_delay),
              ultimate_entries = last(ultimate_entries), 
              lb = last(lb),
              ub = last(ub), 
              interval_type = last(interval_type),
              rmse = mean(sqrt(sum(se))),
              rmsre = round(mean(sqrt(sum(squared_relative_error))) * 100, 2),
              pi_coverage = mean(pi_coverage)) |>
    mutate(reporting_date = event_date + reporting_delay) |>
    relocate(reporting_date, .before = event_date)
  
}

