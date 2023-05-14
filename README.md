# Repository containing utility functions for nowcasting 

## Data preparation 

For Switzerland epidemiological data for Covid-19 can be obtained from the
official API [covid19.admin.ch/api]("https://www.covid19.admin.ch/api/"). 

Daily data for cases and hospitalizations can be downloaded and pre-processed by 
sourcing the `init_db.R` file.

`source("R/init_db.R")` 

This will create and populate a local DuckDB database. 

**Remark**: Filling up the database may take some hours. 