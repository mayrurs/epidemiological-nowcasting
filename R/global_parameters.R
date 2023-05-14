# Pandemic parameters
phases <- tibble::tribble(
~name, ~start, ~end, 
"Phase 1", lubridate::ymd("2020-02-24"), lubridate::ymd("2020-06-07"),
"Phase 2", lubridate::ymd("2020-06-08"), lubridate::ymd("2021-02-14"),
"Phase 3", lubridate::ymd("2021-02-15"), lubridate::ymd("2021-06-20"),
"Phase 4", lubridate::ymd("2021-06-21"), lubridate::ymd("2021-10-10"),
"Phase 5", lubridate::ymd("2021-10-11"), lubridate::ymd("2021-12-19"),
"Phase 6", lubridate::ymd("2021-12-20"), lubridate::ymd("2022-06-11"),
)
