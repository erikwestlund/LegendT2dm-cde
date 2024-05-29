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
vocabularyDatabaseSchema <- cdmDatabaseSchema

# We will pull the necessary sections of execute in ./R/Main.R to generate required data.

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

fetchAllDataFromServer(connectionDetails = connectionDetails,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       oracleTempSchema = oracleTempSchema,
                       cohortDatabaseSchema = cohortDatabaseSchema,
                       tablePrefix = tablePrefix,
                       indicationId = indicationId,
                       outputFolder = outputFolder,
                       studyEndDate = studyEndDate,
                       useSample = FALSE)


generateAllCohortMethodDataObjects(outputFolder = outputFolder,
                                   indicationId = indicationId,
                                   useSample = FALSE,
                                   maxCores = maxCores)

if (runCohortMethod) {
  runCohortMethod(outputFolder = outputFolder,
                  indicationId = indicationId,
                  databaseId = databaseId,
                  maxCores = maxCores,
                  runSections = runSections)
}

# if (computeIncidence) {
#     computeIncidence(outputFolder = outputFolder, indicationId = indicationId)
# }
#
# if (fetchChronographData) {
#     fetchChronographData(connectionDetails = connectionDetails,
#                          cdmDatabaseSchema = cdmDatabaseSchema,
#                          oracleTempSchema = oracleTempSchema,
#                          cohortDatabaseSchema = cohortDatabaseSchema,
#                          tablePrefix = tablePrefix,
#                          indicationId = indicationId,
#                          outputFolder = outputFolder)
# }

if (computeCovariateBalance) {
  computeCovariateBalance(outputFolder = outputFolder,
                          indicationId = indicationId,
                          maxCores = maxCores)
}

if (exportToCsv) {
  exportResults(indicationId = indicationId,
                outputFolder = outputFolder,
                databaseId = databaseId,
                databaseName = databaseName,
                databaseDescription = databaseDescription,
                minCellCount = minCellCount,
                runSections = runSections,
                maxCores = maxCores,
                exportSettings = exportSettings)
}

ParallelLogger::logInfo(sprintf("Finished execute() for LEGEND-T2DM %s-vs-%s studies",
                                indicationId, indicationId))





## END execute() ####


######

# OLD:

# Required to run study sections -- from Main.R
writePairedCounts <- function(outputFolder, indicationId) {

    tcos <- readr::read_csv(file = system.file("settings", paste0(indicationId, "TcosOfInterest.csv"),
                                               package = "LegendT2dm"),
                            col_types = readr::cols())
    counts <- readr::read_csv(file = file.path(outputFolder, indicationId, "cohortCounts.csv"),
                              col_types = readr::cols()) %>%
        select(.data$cohortDefinitionId, .data$cohortCount)

    tmp <- tcos %>%
        left_join(counts, by = c("targetId" = "cohortDefinitionId")) %>% rename(targetPairedPersons = .data$cohortCount) %>%
        left_join(counts, by = c("comparatorId" = "cohortDefinitionId")) %>% rename(comparatorPairedPersons = .data$cohortCount)

    tmp <- tmp %>%
        mutate(targetPairedPersons = ifelse(is.na(.data$targetPairedPersons), 0, .data$targetPairedPersons)) %>%
        mutate(comparatorPairedPersons = ifelse(is.na(.data$comparatorPairedPersons), 0, .data$comparatorPairedPersons))

    readr::write_csv(tmp, file = file.path(outputFolder, indicationId, "pairedExposureSummary.csv"))
}


# Using execute function, let's run necessary parts
indicationFolder <- file.path(outputFolder, indicationId)
if (!file.exists(indicationFolder)) {
  dir.create(indicationFolder, recursive = TRUE)
}

ParallelLogger::addDefaultFileLogger(file.path(indicationFolder, "log.txt"))
ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
on.exit(ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)

sinkFile <- file(file.path(indicationFolder, "console.txt"), open = "wt")
sink(sinkFile, split = TRUE)
on.exit(sink(), add = TRUE)

ParallelLogger::logInfo(sprintf("Starting execute() for LEGEND-T2DM %s-vs-%s studies",
                                indicationId, indicationId))

createExposureCohorts(
  conn,
  cdmDatabaseSchema,
  cdmDatabaseSchema,
  cohortDatabaseSchema,
  tablePrefix,
  indicationId,
  oracleTempSchema,
  outputFolder,
  databaseId,
  filterOutcomeCohorts
)

minCohortSize <- 1000
fetchAllDataFromServer <- TRUE
studyEndDate <- ""

pairedExposureSummaryPath = file.path(indicationFolder, "pairedExposureSummaryFilteredBySize.csv")
if(!file.exists(pairedExposureSummaryPath) || createPairedExposureSummary){
  writePairedCounts(outputFolder = outputFolder, indicationId = indicationId)
  filterByExposureCohortsSize(outputFolder = outputFolder, indicationId = indicationId,
                              minCohortSize = minCohortSize)
}
exposureSummary = read.csv(pairedExposureSummaryPath)

createOutcomeCohorts(
  conn,
  cdmDatabaseSchema,
  cdmDatabaseSchema,
  cohortDatabaseSchema,
  tablePrefix,
  oracleTempSchema,
  outputFolder,
  databaseId,
  filterOutcomeCohorts
)

fetchAllDataFromServer(connectionDetails = connectionDetails,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       oracleTempSchema = oracleTempSchema,
                       cohortDatabaseSchema = cohortDatabaseSchema,
                       tablePrefix = tablePrefix,
                       indicationId = indicationId,
                       outputFolder = outputFolder,
                       studyEndDate = studyEndDate,
                       useSample = useSample)

generateAllCohortMethodDataObjects(outputFolder = outputFolder,
                                   indicationId = indicationId,
                                   useSample = FALSE,
                                   maxCores = maxCores)

if (runCohortMethod) {
  runCohortMethod(outputFolder = outputFolder,
                  indicationId = indicationId,
                  databaseId = databaseId,
                  maxCores = maxCores,
                  runSections = runSections)
}

