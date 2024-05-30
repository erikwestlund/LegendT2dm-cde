# This is adapted from extra/CodeToRunRedShift.R
rJava::.jinit(parameters="-Xmx100g", force.init = TRUE)
options(java.parameters = c("-Xms200g", "-Xmx200g"))

library(LegendT2dm)
library(dbplyr)
library(dplyr)

# Configure and download database drivers. These will need to be unzipped.
databaseDriversDir <- "/Users/e.westlund/Documents/DatabaseDrivers/"
dir.create(databaseDriversDir, showWarnings = FALSE)
Sys.setenv(DATABASECONNECTOR_JAR_FOLDER=databaseDriversDir)

DatabaseConnector::downloadJdbcDrivers(dbms = "redshift")

# Configure temp folders
andromedaTempDir <- "/Users/e.westlund/Documents/AndromedaTemp/"
dir.create(andromedaTempDir, showWarnings = FALSE)
options(andromedaTempFolder = andromedaTempDir)
oracleTempSchema <- NULL

# Results folder
resultsDir <- "/Users/e.westlund/Documents/Legendt2dm-CI-Results"
dir.create(resultsDir, showWarnings = FALSE)

# Configure
cdmDatabaseSchema <- "omop_cdm_53_pmtx_202203"
serverHostname <-
  serverSuffix <- "ohdsilab"
cohortDatabaseSchema <- "work_e_westlund185"
databaseId <- "NEOHDSI"
databaseName <- "Northeastern OMOP CDM"
databaseDescription <- "Northeastern OHDSI Lab OMOP CDM"
tablePrefix <- "legend_t2dm_ne_ohdsilab"
outputFolder <- resultsDir
maxCores <- 2

# Before: Unzip downloaded Redshift drivers and make sure pathToDriver directory is correct
conn <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  server = "ohdsi-lab-redshift-cluster-prod.clsyktjhufn7.us-east-1.redshift.amazonaws.com/ohdsi_lab",
  port = 5439,
  user = "e_westlund185",
  password = "XXXXXX",
  pathToDriver = paste0(databaseDriversDir, "redShiftV1.2.27.1051")
)

indicationId <- "class"
filterOutcomeCohorts <- NULL
filterExposureCohorts <- NULL
oracleTempSchema <- NULL
connectionDetails <- conn
