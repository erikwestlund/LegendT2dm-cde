# This is adapted from extra/CodeToRunRedShift.R
rJava::.jinit(parameters="-Xmx100g", force.init = TRUE)
options(java.parameters = c("-Xms200g", "-Xmx200g"))

library(LegendT2dm)
library(dbplyr)
library(dplyr)
library(keyring)

# Configure and download database drivers. These will need to be unzipped.
databaseDriversDir <- "/Users/e.westlund/Documents/DatabaseDrivers/"
dir.create(databaseDriversDir, showWarnings = FALSE)
Sys.setenv(DATABASECONNECTOR_JAR_FOLDER=databaseDriversDir)

#DatabaseConnector::downloadJdbcDrivers(dbms = "redshift")

# Configure temp folders
andromedaTempDir <- "/Users/e.westlund/Documents/AndromedaTemp/"
dir.create(andromedaTempDir, showWarnings = FALSE)
options(andromedaTempFolder = andromedaTempDir)
oracleTempSchema <- NULL

# Results folder
resultsDir <- "/Users/e.westlund/Documents/Legendt2dmCiResults"
dir.create(resultsDir, showWarnings = FALSE)

# Configure
cdmDatabaseSchema <- "omop_cdm_53_pmtx_202203"
serverHostname <-
serverSuffix <- "ohdsi_lab"
cohortDatabaseSchema <- "work_e_westlund185"
databaseId <- "NEOHDSI"
databaseName <- "Northeastern PharMetrics Plus"
databaseDescription <- "Northeastern PharMetrics Plus OMOP CDM"
tablePrefix <- "legend_t2dm_ci"
outputFolder <- resultsDir
maxCores <- 2

# Before: Unzip downloaded Redshift drivers and make sure pathToDriver directory is correct
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  server = paste0(keyring::key_get("redshiftServer"), "/", !!serverSuffix),
  port = 5439,
  user = keyring::key_get("redshiftUser"),
  password = keyring::key_get("redshiftPassword"),
  pathToDriver = paste0(databaseDriversDir, "redShiftV1.2.27.1051")
)

indicationId <- "ci"
filterOutcomeCohorts <- NULL
filterExposureCohorts <- NULL
oracleTempSchema <- NULL
createPairedExposureSummary <- TRUE
studyEndDate <- ""
minCohortSize <- 0
vocabularyDatabaseSchema <- cdmDatabaseSchema

# We will pull the necessary sections of execute in ./R/Main.R to generate required data.

indicationFolder <- file.path(outputFolder, indicationId)
if (!file.exists(indicationFolder)) {
  dir.create(indicationFolder, recursive = TRUE)
}

# Note: we swap out the indicationId from "class" to "ci" to be customized for what we want to study
# This requires us to create some extra settings files. Namely:
# - settings/ciCohortsToCreate.csv
createExposureCohorts(connectionDetails = connectionDetails,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                      cohortDatabaseSchema = cohortDatabaseSchema,
                      tablePrefix = tablePrefix,
                      indicationId = indicationId,
                      oracleTempSchema = oracleTempSchema,
                      outputFolder = outputFolder,
                      databaseId = databaseId,
                      filterExposureCohorts = filterExposureCohorts,
                      imputeExposureLengthWhenMissing = imputeExposureLengthWhenMissing)

# We now summarize the treatment/comparator pairs as specified in settings/ciTcosOfInterest.csv
pairedExposureSummaryPath = file.path(indicationFolder, "pairedExposureSummaryFilteredBySize.csv")

if(!file.exists(pairedExposureSummaryPath) || createPairedExposureSummary){
  writePairedCounts(outputFolder = outputFolder, indicationId = indicationId)
  filterByExposureCohortsSize(outputFolder = outputFolder,
                              indicationId = indicationId,
                              minCohortSize = minCohortSize)
}
exposureSummary = read.csv(pairedExposureSummaryPath)

# Note that I have modified this function to accept an Outcomes parameter, allowing us
# to pull fewer outcomes.
createOutcomeCohorts(connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     tablePrefix = tablePrefix,
                     outcomesFile = "settings/ciOutcomesOfInterest.csv",
                     oracleTempSchema = oracleTempSchema,
                     outputFolder = outputFolder,
                     databaseId = databaseId,
                     filterOutcomeCohorts = filterOutcomeCohorts)

# Pull down the data locally.
fetchAllDataFromServer(connectionDetails = connectionDetails,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       oracleTempSchema = oracleTempSchema,
                       cohortDatabaseSchema = cohortDatabaseSchema,
                       tablePrefix = tablePrefix,
                       indicationId = indicationId,
                       outputFolder = outputFolder,
                       studyEndDate = studyEndDate,
                       useSample = FALSE)

# Generate CohortMethod data objects
generateAllCohortMethodDataObjects(outputFolder = outputFolder,
                                   indicationId = indicationId,
                                   useSample = FALSE,
                                   maxCores = maxCores)

# Run CohortMethod
# NOTE: This is hacky, but for time saving, if you do not need all the p-score
# analysis, you can run and abort each of these once the data is exported.
extractCohortMethodData(outputFolder = outputFolder,
                        indicationId = indicationId,
                        databaseId = databaseId,
                        maxCores = maxCores,
                        sections = c(1,3)) #ITT, OT2

runCohortMethod(outputFolder = outputFolder,
                indicationId = indicationId,
                databaseId = databaseId,
                maxCores = maxCores,
                runSections = c(1)) #ITT

runCohortMethod(outputFolder = outputFolder,
                indicationId = indicationId,
                databaseId = databaseId,
                maxCores = maxCores,
                runSections = c(3)) #OT2


