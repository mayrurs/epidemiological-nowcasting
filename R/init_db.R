library(DBI)
library(dplyr)
library(purrr)

source("R/db_utils.R")

init_db <- function(con = connect_db()) { 
  
  on.exit(expr = dbDisconnect(con, shutdown=TRUE), after=TRUE)
  
  # Add tables
  sql <- list(
    history = "CREATE TABLE history(reporting_date DATE PRIMARY KEY, 
                                              data_version VARCHAR,
                                              week_day VARCHAR,
                                              published BOOLEAN);",
    
    cases_daily = "CREATE TABLE cases_daily(reporting_date DATE,
                                     event_date DATE,
                                     geoRegion VARCHAR, 
                                     entries INTEGER,
                                     mean7d DOUBLE, 
                                     mean14d DOUBLE,
                                     data_type VARCHAR, 
                                     UNIQUE (reporting_date, event_date, geoRegion, data_type));",
    
    meta_cases_daily = "CREATE TABLE meta_cases_daily(reporting_date DATE,
                                     data_version VARCHAR,
                                     parsed BOOLEAN,
                                     published BOOLEAN,
                                     data_update BOOLEAN,
                                     zero_fill BOOLEAN,
                                     UNIQUE (reporting_date));",
    
    hosps_daily = "CREATE TABLE hosps_daily(reporting_date DATE,
                                     event_date DATE,
                                     geoRegion VARCHAR, 
                                     entries INTEGER,
                                     mean7d DOUBLE,
                                     mean14d DOUBLE,
                                     data_type VARCHAR, 
                                     UNIQUE (reporting_date, event_date, geoRegion, data_type));",
    
    meta_hosps_daily = "CREATE TABLE meta_hosps_daily(reporting_date DATE,
                                     data_version VARCHAR,
                                     parsed BOOLEAN,
                                     published BOOLEAN,
                                     data_update BOOLEAN,
                                     zero_fill BOOLEAN,
                                     UNIQUE (reporting_date));"
  )

  
  # Clean-up old tables 
  walk(names(sql), function(x) 
    {print(x) 
     sql <- glue_sql("DROP TABLE IF EXISTS {`x`}", .con = con)
     dbExecute(con = con, sql)})
  
  # initialize new tables
  walk(sql, ~ dbExecute(con = con, .x))
  
}

# Initalize DB ----
init_db()

# Add history --- 
update_history()

# Populate cases db for published data
update_table("cases_daily", from = "2020-11-05", to = "2022-12-31")
update_table("hosps_daily", from = "2020-11-05", to = "2022-12-31")

