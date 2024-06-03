#' Adopted from runCohortMethod in this package to more cleanly extract data objects we need.
#' @export
extractCohortMethodData <- function(
  outputFolder,
  indicationId = "ci",
  databaseId,
  maxCores = 4,
  includeNegativeControls = FALSE,
  runSections
) {

    # Tell CohortMethod to minimize files sizes by dropping unneeded columns:
    options("minimizeFileSizes" = TRUE)

    indicationFolder <- file.path(outputFolder, indicationId)
    exposureSummary <- read.csv(file.path(indicationFolder,
                                          "pairedExposureSummaryFilteredBySize.csv"))

    pathToCsv <- system.file("settings", "OutcomesOfInterest.csv", package = "LegendT2dm")
    hois <- read.csv(pathToCsv)

    if(includeNegativeControls) {
      pathToCsv <- system.file("settings", "NegativeControls.csv", package = "LegendT2dm")
      negativeControls <- read.csv(pathToCsv)
      outcomeIds <- unique(c(hois$cohortId, negativeControls$cohortId))
    } else {
      outcomeIds <- hois$cohortId
    }

    # First run: ITT
    if (1 %in% runSections) {
        executeSingleCmExtraction(message = "ITT analyses",
                           folder = "ITT",
                           exposureSummary = exposureSummary[isOt1(exposureSummary$targetId), ],
                           cmAnalysisList = system.file("settings", "ittCmAnalysisList.json", package = "LegendT2dm"),
                           outcomeIds = outcomeIds,
                           outcomeIdsOfInterest = hois$cohortId,
                           indicationFolder = indicationFolder,
                           maxCores = maxCores)
    }


    # Second run: OT1
    if (2 %in% runSections) {
        executeSingleCmExtraction(message = "OT1 analyses",
                           folder = "OT1",
                           exposureSummary = exposureSummary[isOt1(exposureSummary$targetId), ],
                           cmAnalysisList = system.file("settings", "ot1CmAnalysisList.json", package = "LegendT2dm"),
                           outcomeIds = outcomeIds,
                           outcomeIdsOfInterest = hois$cohortId,
                           # copyPsFileFolder = "ITT",
                           indicationFolder = indicationFolder,
                           maxCores = maxCores)
    }

    # Third run: OT2
    if (3 %in% runSections) {
        executeSingleCmExtraction(message = "OT2 analyses",
                           folder = "OT2",
                           exposureSummary = exposureSummary[!isOt1(exposureSummary$targetId), ],
                           cmAnalysisList = system.file("settings", "ot2CmAnalysisList.json", package = "LegendT2dm"),
                           outcomeIds = outcomeIds,
                           outcomeIdsOfInterest = hois$cohortId,
                           # copyPsFileFolder = "ITT",
                           # convertPsFileNames = TRUE,
                           indicationFolder = indicationFolder,
                           maxCores = maxCores)
    }
}

#' @export
executeSingleCmExtraction <- function(message,
                               folder,
                               exposureSummary,
                               cmAnalysisListFile,
                               outcomeIds,
                               outcomeIdsOfInterest,
                               copyPsFileFolder = "",
                               convertPsFileNames = FALSE,
                               indicationFolder,
                               maxCores) {

  ParallelLogger::logInfo(paste0("Executing CohortMethod Data Extraction for ", message))

  runCmFolder <- file.path(indicationFolder, "cmOutput", folder)
  if (!dir.exists(runCmFolder)) {
    dir.create(runCmFolder, recursive = TRUE)
  }

  copyCmDataFiles(exposureSummary,
                  file.path(indicationFolder, "cmOutput"),
                  runCmFolder)

  runTcoList <- lapply(1:nrow(exposureSummary), function(i) {
    CohortMethod::createTargetComparatorOutcomes(targetId = exposureSummary[i,]$targetId,
                                                 comparatorId = exposureSummary[i,]$comparatorId,
                                                 outcomeIds = outcomeIds)
  })
}

#' @export
copyCmDataFiles <- function(exposures, source, destination) {
  lapply(1:nrow(exposures), function(i) {
    fileName <- file.path(source,
                          sprintf("CmData_l1_t%s_c%s.zip",
                                  exposures[i,]$targetId,
                                  exposures[i,]$comparatorId))
    success <- file.copy(fileName, destination, overwrite = TRUE,
                         copy.date = TRUE)
    if (!success) {
      stop("Error copying file: ", fileName)
    }
  })
}
