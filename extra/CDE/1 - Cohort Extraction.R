# Make sure to run all code until "END ENVIRONMENT PREP"
# START ENVIRONMENT PREP
rJava::.jinit(parameters="-Xmx100g", force.init = TRUE)
options(java.parameters = c("-Xms200g", "-Xmx200g"))

library(LegendT2dm)
library(dbplyr)
library(dplyr)
library(purrr)
library(stringr)
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
resultsDir <- "/Users/e.westlund/Documents/Legendt2dmCdeOutput"
dir.create(resultsDir, showWarnings = FALSE)

# Configure
cdmDatabaseSchema <- "omop_cdm_53_pmtx_202203"
serverHostname <-
  serverSuffix <- "ohdsi_lab"
cohortDatabaseSchema <- "work_e_westlund185"
databaseId <- "NEOHDSI"
databaseName <- "Northeastern PharMetrics Plus"
databaseDescription <- "Northeastern PharMetrics Plus OMOP CDM"
tablePrefix <- "legend_t2dm_cde"
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

indicationId <- "cde"
filterOutcomeCohorts <- NULL
filterExposureCohorts <- NULL
oracleTempSchema <- NULL
createPairedExposureSummary <- TRUE
studyEndDate <- ""
minCohortSize <- 0
vocabularyDatabaseSchema <- cdmDatabaseSchema

# END ENVIRONMENT PREP

# START DATA EXTRACTION
# The below can be run in steps and do not need to be run each time.

# STEP 1
# We will pull the necessary sections of execute in ./R/Main.R to generate required data.
indicationFolder <- file.path(outputFolder, indicationId)
if (!file.exists(indicationFolder)) {
  dir.create(indicationFolder, recursive = TRUE)
}

# STEP 2
# Note: we swap out the indicationId from "class" to "cde" to be customized for what we want to study
# This requires us to create some extra settings files. Namely:
# - settings/cdeCohortsToCreate.csv
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

# STEP 3
# We now summarize the treatment/comparator pairs as specified in settings/ciTcosOfInterest.csv
pairedExposureSummaryPath = file.path(indicationFolder, "pairedExposureSummaryFilteredBySize.csv")

if(!file.exists(pairedExposureSummaryPath) || createPairedExposureSummary){
  writePairedCounts(outputFolder = outputFolder, indicationId = indicationId)
  filterByExposureCohortsSize(outputFolder = outputFolder,
                              indicationId = indicationId,
                              minCohortSize = minCohortSize)
}
exposureSummary = read.csv(pairedExposureSummaryPath)

# STEP 4
# Note that I have modified this function to accept an Outcomes parameter, allowing us
# to pull fewer outcomes.
createOutcomeCohorts(connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     tablePrefix = tablePrefix,
                     outcomesFile = "settings/cdeOutcomesOfInterest.csv",
                     oracleTempSchema = oracleTempSchema,
                     outputFolder = outputFolder,
                     databaseId = databaseId,
                     filterOutcomeCohorts = filterOutcomeCohorts)

# STEP 5
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

# STEP 6
# Generate CohortMethod data objects
generateAllCohortMethodDataObjects(outputFolder = outputFolder,
                                   indicationId = indicationId,
                                   useSample = FALSE,
                                   maxCores = maxCores)

# STEP 7
extractCohortMethodData(outputFolder = outputFolder,
                        indicationId = indicationId,
                        databaseId = databaseId,
                        maxCores = maxCores,
                        runSections = c(1,2,3)) #ITT, OT1, OT3

# STEP 8 - Create Backup and Clean up up
tar(paste0(outputFolder, "-backup.tgz"), outputFolder, compression="gzip")

# Delete all covariates file
unlink(file.path(outputFolder, indicationId, "allCovariates.zip"))

# Delete redundant cohort data and stray analysis directories in analysis directories
lapply(c('ITT', 'OT1', 'OT2'), function(analysis) {
  list.files(file.path(outputFolder, indicationId, "cmOutput", analysis), full.name=TRUE)  %>%
    stringr::str_subset(pattern="CmData_") %>%
    unlink()

  list.dirs(file.path(outputFolder, indicationId, "cmOutput", analysis)) %>%
    stringr::str_subset(pattern="Analysis_") %>%
    unlink(force=TRUE, recursive=TRUE)
})

# Keep only data for SGLT2I vs SU and GLP1RA vs SU
# GLP1RA main   201100000            SU main  401100000
# SGLT2I main   301100000            SU main  401100000
# GLP1RA main ot2  202100000            SU main ot2 402100000
# SGLT2I main ot2  302100000            SU main ot2 402100000
cohortComparisonsToKeep <- list(
  list(t="201100000", c="401100000"),
  list(t="301100000", c="401100000"),
  list(t="202100000", c="402100000"),
  list(t="302100000", c="402100000")
)

# Delete unneeded cohort files in "allCohorts"
cohortFiles <- list.files(file.path(outputFolder, "cde", "allCohorts")) %>%
  stringr::str_subset(pattern="cohorts_")
keepFiles <- map_chr(cohortComparisonsToKeep, function(pair) {
  paste0("cohorts_t", pair$t, "_c", pair$c, ".zip")
})
file.path(paste(outputFolder, "cde", "allCohorts", cohortFiles[!(cohortFiles %in% keepFiles)], sep="/")) %>%
  unlink()

# Delete unneeded cohort files in CmOutput with covariates included
cohortFiles <- list.files(file.path(outputFolder, indicationId, "cmOutput"))  %>%
  stringr::str_subset(pattern="CmData_")
keepFiles <- map_chr(cohortComparisonsToKeep, function(pair) {
  paste0("CmData_l1_t", pair$t, "_c", pair$c, ".zip")
})
file.path(paste(outputFolder, "cde", "cmOutput", cohortFiles[!(cohortFiles %in% keepFiles)], sep="/")) %>%
  unlink()

# Delete unneeded study population files
lapply(c('ITT', 'OT1', 'OT2'), function(analysis) {
  prefixesToKeep <- map_chr(cohortComparisonsToKeep, function(pair) {
    paste0("StudyPop_l1_s1_t", pair$t, "_c", pair$c)
  })
  studyPopulationFiles <- list.files(file.path(outputFolder, indicationId, "cmOutput", analysis), full.name=TRUE)

  filesToKeep <- studyPopulationFiles[grep(paste(prefixesToKeep, collapse = "|"), studyPopulationFiles)]

  studyPopulationFiles[!(studyPopulationFiles %in% filesToKeep)] %>%
    unlink()
})

# Compress.
tar(paste0(outputFolder, "-extracted.tgz"), outputFolder, compression="gzip")
