
# Welcome to ohdsitargets!
# The current targets script is for the "Hello OHDSI" example 
# Every {ohdsitargets} pipeline needs a _targets.R file. You may
# use this to get acquainted with the package or edit to your own study

# To run the pipeline use targets::tar_make(). To review the pipeline run
# targets::tar_visnetwork. To look at results from the pipeline use
# targets::tar_read(<target>), where the bracketed item is replaced with 
# a suitable target in the ohdsitargets pipeline.



# Prepare Environment ----------------------

# Load packages required to define the pipeline:
library(ohdsitargets)
library(targets)
library(dplyr)

# source("~/R/github/ohdsiTargets/R/pipeline_setup.R")
# source("~/R/github/ohdsiTargets/R/generate.R")
# Set target options:
tar_option_set(
  packages = c("tibble", "dplyr", "CirceR", "Capr",
               "CohortGenerator", "here", "DatabaseConnector"), 
  format = "rds" # default storage format
  # Set other options as needed.
)



## Setup Pipeline ----------------------

#define execution settings
executionSettings <- create_execution_settings(active_database = "eunomia")
#define cohorts to create
cohortsToCreate <- readr::read_csv("input/cohorts/meta/CohortsToCreate.csv", show_col_types = F) 
#define incidence Analysis
incidenceAnalysisSettings <- tibble::tibble(firstOccurrenceOnly = TRUE,
                                            washoutPeriod = 365)
temporalCovariateSettings <- define_temporal_covariates()

# Run Targets -----------------------

list(
  #Step1: Database Diagnostics
  tar_database_diagnostics(executionSettings = executionSettings),
  #step2: generate cohorts
  tar_cohorts(cohortsToCreate = cohortsToCreate,
             executionSettings = executionSettings),
  #step 3: Run cohort diagnostics
  tar_cohort_diagnostics(cohortsToCreate = cohortsToCreate,
                         executionSettings = executionSettings,
                         incidenceAnalysisSettings = incidenceAnalysisSettings,
                         temporalCovariateSettings = temporalCovariateSettings)
)
