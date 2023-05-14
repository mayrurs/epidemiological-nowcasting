library(DBI)
library(digest)
library(jsonlite)
library(dplyr)
library(fs)
library(magrittr)
library(lubridate)
library(purrr)
library(tidyr)
library(glue)
library(ggplot2)
library(stringr)

source("db_utils.R")
source("data_preprocessing.R")

delay_window <- 20
triangle <- cumulative_triangle(delay_window = delay_window)

# Chain ladder 
## Compute cumulative cases
C_ij<- m |> rowwise() |> 
  accumulate(`+`) |> 
  as_tibble()

## Weekly data (Tuesday publication date)
con <- connect_db()
history <- dbReadTable(con, 'history') |>
  filter(week_day == "Tue")

current <- get_data('2020-11-17', table = "cases_daily", "new_data", con) %>% 
  mutate(week = isoweek(date), 
         reporting_week = isoweek(reporting_date)) %>% 
  group_by(week, reporting_week) %>% 
  summarise(entries = sum(entries))

next_week  <- get_data('2020-11-24', table = "cases_daily", "new_data", con) %>% 
  mutate(week = isoweek(date), 
         reporting_week = isoweek(reporting_date)) %>% 
  group_by(week, reporting_week) %>% 
  summarise(entries = sum(entries))


