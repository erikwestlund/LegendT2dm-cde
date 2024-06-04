library(Andromeda)
library(FeatureExtraction)
library(RSQLite)
library(purrr)

resultsDir <- "/Users/e.westlund/Documents/LegendT2dmCiOutput/ci"

# Outcome summary
outcomeSummary <- read.csv("inst/settings/OutcomesOfInterest.csv") %>%
  dplyr::select(cohortId, atlasName)
outcomeSummary

# Paired exposure summary:
pairedExposureSummary <- read.csv(paste(resultsDir, "pairedExposureSummary.csv", sep="/"))
pairedExposureSummary

# All Covariates
covariates <- FeatureExtraction::loadCovariateData(paste(resultsDir, "allCovariates.zip", sep="/"))

covariates$covariates
covariates$covariateRef
covariates$analysisRef

# Tidied covariates
tidyCovariates <-
  FeatureExtraction::tidyCovariateData(
    covariates,
    minFraction = 0.001,
    normalize = TRUE,
    removeRedundancy = TRUE
  )

## Getting started example: ITT
# Target: DPP4I main
# Comparator: GLP1RA main
# Outcome: 3pt MACE
cohort <- Andromeda::loadAndromeda(paste(resultsDir, "cmOutput/ITT/CmData_l1_t101100000_c201100000.zip", sep="/"))
cohort$analysisRef
cohort$covariateRef
cohort$covariates

studyPop <- readRDS(paste(resultsDir, "cmOutput/ITT/StudyPop_l1_s1_t101100000_c201100000_o1.rds", sep="/"))
