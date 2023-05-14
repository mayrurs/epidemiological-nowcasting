library(DBI)
library(dplyr)
library(lubridate)
library(glue)
        
#' Utility function to return default duckdb connection
#'
#' @param dbdir 
#'
#' @return
#' @export
#'
#' @examples
connect_db <- function(dbdir = 'covid19.duckdb') { 
  
  con <-DBI::dbConnect(duckdb::duckdb(), dbdir = dbdir, read_only = FALSE)
  
 }

#' Helper function to download json file from official foph API
#'
#' @param data_version 
#' @param file_name 
#'
#' @return
#' @export
#'
#' @examples
get_json <- function(data_version, file_name = 'COVID19Cases_geoRegion') {
  
  base_url <- "https://www.covid19.admin.ch/api/data/" 
  url <- paste0(base_url, data_version, "/sources/", file_name, '.json')
  data <- jsonlite::fromJSON(url, flatten=TRUE)
  
}

download_and_save_foph <- function(reporting_date, file_name = 'COVID19Cases_geoRegion', path = NULL, ext = "csv", con = connect_db()) { 
 
  on.exit(dbDisconnect(con, shutdown = TRUE))  
  
  data_version <- dbGetQuery(con, glue_sql("SELECT data_version FROM history WHERE reporting_date = {reporting_date}", .con = con))$data_version
  
  if (length(data_version) == 0) {
    cat("No published data for ", reporting_date, " available")
  } else { 
    data <- get_json(data_version, file_name) 
    file_name <- paste0(as.character(as_date(unique(data$version))), "_", file_name, ".", ext)
    
    if (is.null(path)) {
      path <- fs::path(".", "raw", file_name)
    } else {
      path <- fs::path(path, file_name)
    }
    
    switch(ext,
           csv = readr::write_csv(data, path),
           json = jsonlite::write_json(data, path)
    )
  }
}
  

#' Helper to parse all published data versions from foph API
#'
#' @return
#' @export
#'
#' @examples
get_history <- function() { 
  
  data_context <- jsonlite::fromJSON("https://www.covid19.admin.ch/api/data/context/history", 
                           flatten=TRUE)$dataContexts %>%
    filter(latest == TRUE) %>% 
    select(date,
           data_version = dataVersion) %>% 
    mutate(date = as_date(date))

}

#' Update publication history in local database
#'
#' @param con 
#'
#' @return
#' @export
#'
#' @examples
update_history <- function(con = connect_db()) { 
  
  # Not yet implemented
  history <- get_history() # Get newest history 
  
  # Complete time series
  history <- history |> 
    tidyr::complete(date = seq(min(date), 
                        max(date), 
                        by = "day"))
  
  # Add metadata
  history <- history |>
    mutate(week_day = wday(date, label = TRUE), # Week-day of reporting
           data_version = if_else(week_day %in% c("Sat", "Sun"), NA, data_version),
           published = if_else(is.na(data_version), FALSE, TRUE)) |>
    rename(reporting_date = date)
  
  dbWriteTable(con, "history", history, append = TRUE)

}

#' Download published data from foph api, store in local database
#'
#' @param date 
#' @param table 
#' @param fill_missing # Parameter how to handle non-published data states
#' @param truncate_date # By default only keeps data for which a delay can be computed
#' @param con 
#'
#' @return
#' @export
#'
#' @examples
add_data <- function(reporting_date, table = "cases_daily", 
                     fill_missing = c("forward_zero"),
                     truncate_date = "2020-11-05", con = connect_db()) { 
  
  sql <- glue_sql("SELECT reporting_date, data_version, published FROM history WHERE reporting_date = {reporting_date}", .con = con)
  meta_data <- dbGetQuery(con, sql)
  
  key_map <- list(cases_daily = 'COVID19Cases_geoRegion', 
                  hosps_daily = "COVID19Hosp_geoRegion") 
  
  if (!is.na(meta_data$data_version)) { 
   
    data <- get_json(meta_data$data_version, file_name = key_map[[table]]) %>% 
      select(geoRegion, event_date = datum, entries, mean7d, mean14d) %>% 
      mutate(reporting_date = as_date(reporting_date),
             event_date = as_date(event_date)) |>
      relocate(reporting_date, .before = geoRegion)
    
    if (!is_empty(truncate_date)) {
      data <- data %>% 
        filter(event_date >= truncate_date)
    }
    
    # Only add data if there is a new event entry for the reporting_date
    # : API shows sometimes already published data as new data
    if (max(data$event_date) == reporting_date) { 
      data_update = TRUE 
      zero_fill = FALSE
      data <- data %>% 
        mutate(data_type = "new_data")
      cat(paste0(reporting_date, ": Published data added \n"))
    } else { 
      data_update = FALSE
      zero_fill = TRUE
      data <- zero_propagate(reporting_date = reporting_date, table = table, con)
      cat(paste0(reporting_date, ": Published data contains old data, propagate previous values \n"))
      }
    
  } else { 
    data_update = FALSE
    zero_fill = TRUE
    data <- zero_propagate(reporting_date = reporting_date, table = table, con) 
    cat(paste0(reporting_date, ": Data not published, propagate previous values \n"))
    }

  # Update meta information
  sql_meta <- glue_sql("INSERT INTO meta_{`table`} (reporting_date, data_version, parsed, published, data_update, zero_fill) 
                        VALUES ({meta_data$reporting_date}, {meta_data$data_version}, TRUE, {meta_data$published}, {data_update}, {zero_fill})", .con = con)

  dbBegin(con) 
  dbWriteTable(con, table, data, append = TRUE) 
  dbExecute(con, sql_meta) 
  dbCommit(con)
  
}

#' Retrieve data from local database
#'
#' @param date 
#' @param table 
#' @param data_type 
#' @param con 
#'
#' @return
#' @export
#'
#' @examples
get_data <- function(reporting_date, table, data_type, con) { 
  
  sql <- glue_sql("
    SELECT *
    FROM {`table`} 
    WHERE reporting_date = {reporting_date} AND data_type = {data_type};",
                  .con = con)
  
  df <- dbGetQuery(con, sql) 
}


# Batch process download and write to local databaes
update_table <- function(table = "cases_daily", con = connect_db(), from = NULL, to = NULL) {
  
  on.exit(expr = dbDisconnect(con, shutdown=TRUE), after=TRUE)
  
  # Remove already added dates
  to_add_query <- glue_sql("SELECT reporting_date FROM history EXCEPT SELECT reporting_date FROM meta_{`table`}", .con = con)
  to_add <- dbGetQuery(con, to_add_query, .con = con) |> 
    as_tibble()
  
  if (is.null(from)) {
    from <- as_date(to_add[[1, 1]])
  } else {
    from <- as_date(from) 
  #  stopifnot("From date not in range of available FOPH data" = from >= as_date(to_add[[1,1]]))
  }
  
  if (is.null(to)) {
    to <- as_date(to_add[[nrow(to_add), 1]])
  } else {
    to <- as_date(to) 
    stopifnot("To date not in range of available FOPH data" = to <= as_date(to_add[[nrow(to_add), 1]]))
  }
  
  to_add <- to_add |>
    filter(reporting_date >= from, 
           reporting_date <= to) |>
    arrange(reporting_date)
  
  walk(to_add[['reporting_date']], ~add_data(.x, table = table, con = con))
}

#' Handle non-published data by forward propagation
#'
#' @param date 
#' @param table 
#' @param con 
#'
#' @return
#' @export
#'
#' @examples
zero_propagate <- function(reporting_date, table = "cases_daily", con) { 

  current_date <- as_date(reporting_date) 
  previous_date <- current_date - 1
  
  previous_data_type <- dbGetQuery(con, glue_sql("SELECT reporting_date, parsed, data_update, zero_fill,
                    FROM meta_{`table`}", 
                    .con = con)) %>% 
    filter(reporting_date == previous_date)
  
  # CASE I: Previous data where correctly published
  if (previous_data_type$data_update == TRUE) {
    
    data_type = "new_data"
    
  } else if (previous_data_type$zero_fill == TRUE) { 
   
    data_type = "zero_fill" 
    
  } else {
      stop("Data can not be propagated based on previous value")
  }
  
  data <- get_data(previous_date, table = table, data_type, con = con) %>% 
    bind_rows(tibble(reporting_date = current_date, 
                     event_date = current_date,
                     geoRegion = c("CH", "CHFL", "AG", "AR", "AI", "BE", "BL", "BS", 
                                                         "FL", "FR", "GE", "GL", "GR", "JU", "LU", "NE", 
                                                         "NW", "OW", "SG", "SH", "SO", "SZ", "TG", "TI",
                                                         "UR", "VD", "VS", "ZG", "ZH"),
                     entries = 0, 
                     mean7d = NA,
                     mean14d = NA)) %>% 
    mutate(reporting_date = current_date,
           data_type = "zero_fill") 
  }

#' Identify published data states
#'
#' @param date 
#' @param table 
#' @param con 
#'
#' @return
#' @export
#'
#' @examples
has_published <- function(reporting_date, table, con) { 
  
  sql <- glue_sql("SELECT * 
                  FROM meta_{`table`}
                  WHERE reporting_date = {reporting_date} AND published = TRUE AND data_update = TRUE", 
                  .con = con)
  
    nrow(dbGetQuery(con, sql)) > 0
}

#' Inform about downloaded data version in local database
#'
#' @param con 
#' @param table 
#'
#' @return
#' @export
#'
#' @examples
parsed_range <- function(con, table) { 
  
  # Returns dates for which data has been parsed
  sql <- glue_sql("SELECT reporting_date 
                  FROM meta_{`table`}",
                  .con = con)
  
  out <- dbGetQuery(con, sql)
  
}

