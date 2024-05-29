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
resultsDir <- "/Users/e.westlund/Documents/Legendt2dmResults"
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

