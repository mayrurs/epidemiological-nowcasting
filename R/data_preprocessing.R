library(dplyr)
library(purrr)


#' Adds data for date from local db to cumulative count 
#'
#' @param date 
#' @param con 
#' @param table 
#' @param delay_window 
#'
#' @return
#' @export
#'
#' @examples
add_current <- function(reporting_date, .geoRegion, con,  table = "cases_daily",  max_delay = 10, verbose = TRUE) { 
  
  current_date <- as_date(reporting_date)
  
  if (has_published(current_date, table, con)) data_type_current = "new_data" else data_type_current = "zero_fill"
  if (verbose) cat("Date: ", as.character(reporting_date), " Data type: ", data_type_current, "\n")
  
  current <- get_data(reporting_date, table, data_type_current, con) |>
    filter(geoRegion == .geoRegion) |>
    group_by(event_date, geoRegion) |>
    mutate(delay = reporting_date - event_date) |>
    filter(delay <= max_delay) |>
    ungroup() |>
    select(reporting_date, event_date, delay, entries)
  
}

#' Title
#' Return range[1] - observation window + 1 (To allow a full trapzoid over this window back from range[1])
#' @param delay_window # Max delay days
#' @param lag # Shift start of the delay by n days (e.g. start at delay 1)
#' @param con 
#' @param table 
#' @param range 
#'
#' @return
#' @export
#'
#' @examples
cumulative_counts <- function(max_delay,
                              observation_window = max_delay, 
                              .geoRegion = "CH", 
                              con = connect_db(),
                              table = "cases_daily", 
                              range = NULL) { 
    
    on.exit(dbDisconnect(con, shutdown = TRUE))
    stopifnot("Observation windows needs to be bigger or equal to max delay" = observation_window >= max_delay)
  
    if (!is.null(range)) { 
      range <- as_date(range)
      first_entry <- range[1] - observation_window
      last_entry <- range[2] + max_delay
      if (!all(c(as_date(first_entry, last_entry)) %in% parsed_range(con, table)$reporting_date)) { 
        stop("Not all Data for the provided range in given observation window with max delay are not stored in the local database") 
      } else {
        t_n <- tibble::tibble(reporting_date = seq(from = first_entry, to = last_entry, by = "day"))
      }
    } else {
      stop("Not yet implemented")
    }
  
    long_triangle <- map_df(t_n$reporting_date, add_current, .geoRegion = .geoRegion,
                            con = con, 
                            table = table,
                            max_delay = max_delay) |>
      filter(reporting_date >= first_entry, 
             event_date <= range[2]) |>
      group_by(reporting_date) |>
      arrange(delay, .by_group = TRUE) |>
      ungroup()
    
}

select_observation_window <- function(reporting_date_latest, 
                                      cc_long,  
                                      observation_window = 30, 
                                      min_delay = 0, 
                                      max_delay = max(cc_long$delay)) { 
 
  reporting_date_latest <- as_date(reporting_date_latest) 
  
  max_event_date <- reporting_date_latest - min_delay
  min_event_date <- reporting_date_latest - observation_window - min_delay + 1
  
  cc_sub <- cc_long |> 
    filter(reporting_date <= reporting_date_latest, 
           event_date >= min_event_date, 
           delay >= min_delay, 
           delay <= max_delay)
  
  }
  
  
to_triangle_matrix <- function(cc_long) { 
 
  df_wide <- cc_long |>
    select(event_date, delay, entries) |>
    tidyr::pivot_wider(names_from = delay, values_from = entries, 
                       names_sort = TRUE) |>
    arrange(event_date)
  
  data <- as.matrix(df_wide[, 2:length(df_wide)])
  
  row_names <- as.character(df_wide$event_date) 
  col_names <- names(df_wide)[2:length(df_wide)]
  
  dimnames(data) <- list(event_date = row_names,
                         delay = col_names)
  data 

}

to_weight_matrix <- function(cc_long, weight = 0) { 
  
  df_wide <- cc_long |>
    mutate(weights = if_else(lubridate::wday(reporting_date, label = TRUE) %in% c("Sat", "Sun"), weight, 1)) |>
    select(event_date, delay, weights) |> 
    tidyr::pivot_wider(names_from = delay, values_from = weights, 
                       names_sort = TRUE) |>
    arrange(event_date)
  
  data <- as.matrix(df_wide[, 2:length(df_wide)])
  
  row_names <- as.character(df_wide$event_date) 
  col_names <- names(df_wide)[2:length(df_wide)]
  
  dimnames(data) <- list(event_date = row_names,
                         delay = col_names)
  
  data
}

cumulative_to_incremental <- function(cc_long) { 
  
  inc_long <- cc_long %>% 
    group_by(event_date) %>% 
    arrange(delay, .by_group = TRUE) %>% 
    mutate(inc = entries - lag(entries)) %>% 
    left_join(cc_long |> filter(as.integer(delay) == min(as.integer(delay))) |> select(-reporting_date), by = c("event_date", "delay")) %>% 
    mutate(inc = coalesce(inc, entries.y)) %>% 
    rename(entries = inc) %>% 
    select(-entries.y, -entries.x) |>
    tidyr::replace_na(list(entries = 0)) |>
    ungroup()
  
  }

wipe_trapezoid <- function(trapezoid) {
  
  D <- ncol(trapezoid)
  T <- nrow(trapezoid)
  
  stopifnot("tr musst by in triangle or trapezoid form" = T >= D)
  # Separate reporting triangle
  if ((T - D) > 0) {
    utr <- trapezoid[1:(T - D), ]
    tr <- trapezoid[(T - D + 1):T, ]
  } else {
    tr <- trapezoid
  }
  
  # Wipe triangle
  tri <- upper.tri(tr, diag = TRUE)[, D:1]
  tr[!tri] <- NA
  
  # Recreate trapezoid
  if ((T -D) > 0) { 
    tr <- rbind(utr, tr)
  }
  tr 
  
}

#' Helper to extract a subset out of a triangle
#' @param date 
#' @param triangle 
#' @param trapezoid_size 
#' @param delay_prediction 
#' @param count 
#' @param f 
#' @param ... 
#'
#' @return
#' @export
#'
#' @examples
extract_trapezoid <- function(date, triangle, trapezoid_size, fun, delay_prediction = ncol(triangle, ...)) { 
  
  range <- seq(from = as_date(date) - trapezoid_size + 1, to = as_date(date), by = 'day')
  triangle <- triangle[as.character(range), ]
  triangle <- wipe_triangle(triangle, ...)
  
}

#' Return latest reported count from reporting trapezoid
#'
#' @param tr 
#'
#' @return
#' @export
#'
#' @examples
data_by_event_index <- function(tr) { 
  
  D <- ncol(tr) 
  T <- nrow(tr)
  
  c_dt <- vector(length = D)
  for (d in 1:D) {
    c_dt[D - d + 1] <- tr[T - d + 1, d]
  }
  
  df <- tibble(event_date = as_date(rownames(tr)[D:(D-d + 1)]),
               entries = c_dt[length(c_dt):1])
  
}

from_triangle_matrix <- function(triangel, values_drop_na = TRUE) {
  
  long <- triangel |>
    as_tibble() |>
    mutate(event_date = as_date(rownames(triangel))) |>
    pivot_longer(cols=matches("[[:digit:]]"), names_to = "delay", values_to = "entries", 
                 values_drop_na = values_drop_na) |>
    mutate(delay = as.integer(delay),
           reporting_date = event_date + delay) |>
    relocate(reporting_date, .before = event_date) |>
    group_by(reporting_date) |>
    arrange(reporting_date, delay, .by_group = TRUE) |>
    ungroup()
  
}
