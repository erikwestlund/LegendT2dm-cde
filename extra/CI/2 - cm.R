library("RSQLite")
library("purrr")

resultsDir <- "/Users/e.westlund/Documents/Legendt2dmResults"
sqliteFile <- paste0(resultsDir, "/class/cmOutput/CmData_l1_t101100000_c201100000/file2d87eae3a65.sqlite")

sqliteConn <- RSQLite::dbConnect(drv = RSQLite::SQLite(), dbname = sqliteFile)

tables <- dbListTables(sqliteConn)
print(tables)

data_analysisRef <- RSQLite::dbGetQuery(conn = sqliteConn, statement = "SELECT * FROM analysisRef")
