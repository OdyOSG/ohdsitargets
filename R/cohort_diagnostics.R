
#######################################
#Incidence----------------------------
#######################################
##FUnction

#' Incidence analysis for cohort diagnostics
#' 
#' Incidence stratified by age, gender and index year used in cohort diagnostics
#'
#' @param connectionDetails ConnectionDetails
#' @param cdmDatabaseSchema (character) Schema where the CDM data lives in the database
#' @param tempEmulationSchema the temporary table schema used in certain databases
#' @param vocabularyDatabaseSchema (character) Schema where the vocabulary tables lives in the database
#' @param generatedCohort dependency object of generated cohort class that tracks cohort used in incidence
#' analysis
#' @param cdmVersion the version of the cdm >= 5
#' @param firstOccurrenceOnly a logic toggle for first occurrence
#' @param washoutPeriod number of days for washout
#' @return A dataframe with incidence calculations with several combinations
#' @export
cohort_diagnostics_incidence <- function(connectionDetails,
                                         cdmDatabaseSchema = config::get("cdmDatabaseSchema"),
                                         tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                         vocabularyDatabaseSchema =config::get("vocabularyDatabaseSchema"),
                                         generatedCohort,
                                         cdmVersion = 5, 
                                         firstOccurrenceOnly,
                                         washoutPeriod) {
  
  
  cohortTableNames <- generatedCohort$cohortTableRef$cohortTableNames
  cohortDatabaseSchema <- generatedCohort$cohortTableRef$cohortDatabaseSchema
  cohortTable <- generatedCohort$cohortTableRef$cohortTableNames$cohortTable
  cohortId <- generatedCohort$cohort_id
  
  incidenceRate <- CohortDiagnostics:::getIncidenceRate(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cdmVersion = cdmVersion,
    firstOccurrenceOnly = firstOccurrenceOnly,
    washoutPeriod = washoutPeriod,
    cohortId = cohortId
  )
  
  return(incidenceRate)
  
}

### Factory

#' Creates targets of incidence for each cohort in the project
#'
#' @param cohortsToCreate A dataframe with one row per cohort and the following columns: cohortId, cohortName, cohortJsonPath
#' @param incidenceAnalysisSettings a dataframe where each row is a different combination of anlayis
#' @param executionSettings An object containing all information of the database connection created from config file
#' @return One target for each generated cohort with names cohortIncidence_{id}
#' @export
tar_cohort_incidence <- function(cohortsToCreate,
                                 incidenceAnalysisSettings,
                                 executionSettings) {
  
  #extract out all execution settings
  connectionDetails <- executionSettings$connectionDetails
  cdmDatabaseSchema <- executionSettings$cdmDatabaseSchema
  vocabularyDatabaseSchema <- executionSettings$vocabularyDatabaseSchema
  databaseId <- executionSettings$databaseId
  
  
  nn <- 1:nrow(cohortsToCreate)
  
  gg <- tibble::tibble('generatedCohort' = rlang::syms(paste("generatedCohort", nn, sep ="_")),
                       'cohortId' = nn)
  
  iter <- bind_cols(gg, 
                    incidenceAnalysisSettings)
  
  list(
    tarchetypes::tar_map(values = iter, 
                         names = "cohortId",
                         tar_target_raw("cohortIncidence", 
                                        substitute(
                                          cohort_diagnostics_incidence(
                                            connectionDetails = connectionDetails,
                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                            vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                                            generatedCohort = generatedCohort,
                                            firstOccurrenceOnly = firstOccurrenceOnly,
                                            washoutPeriod = washoutPeriod)
                                        )
                         )
    )
  )
  
}


#######################################
#Inclusion Rules-----------------------
#######################################

##Utils

getStatsTable <- function(connectionDetails,
                          cohortDatabaseSchema,
                          table,
                          cohortId,
                          databaseId) {
  suppressMessages(connection <- DatabaseConnector::connect(connectionDetails = connectionDetails))
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  sql <- "SELECT {@database_id != ''}?{CAST('@database_id' as VARCHAR(255)) as database_id,} * 
  FROM @cohort_database_schema.@table 
  WHERE cohort_definition_id = @cohort_id"
  data <- DatabaseConnector::renderTranslateQuerySql(
    sql = sql,
    connection = connection,
    snakeCaseToCamelCase = FALSE,
    table = table,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_id = cohortId,
    database_id = ifelse(is.null(databaseId), yes = "", no = databaseId)
  )
  return(data)
}

### Function

#' Inclusion Rule analysis for cohort diangostics
#' 
#' Extract information on inclusion rules
#'
#' @param connectionDetails ConnectionDetails
#' @param generatedCohort dependency object of generated cohort class that tracks cohort used in incidence
#' analysis
#' @param databaseId (character) identify which database is being used
#' @return A list of dataframes retrieving inclusion rule states generated for the cohorts
#' @export
cohort_diagnostics_inclusion_rules <- function(connectionDetails,
                                               generatedCohort,
                                               databaseId = config::get("databaseName")) {
  
  
  #get names from generated Cohort obj
  cohortTableNames <- generatedCohort$cohortTableRef$cohortTableNames
  #remove cohort inclusion table
  cohortTableNames$cohortInclusionTable <- NULL
  
  cohortDatabaseSchema <- generatedCohort$cohortTableRef$cohortDatabaseSchema
  cohortId <- generatedCohort$cohort_id
  
  
  statTables <- purrr::map(cohortTableNames, ~getStatsTable(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    table = .x,
    cohortId = cohortId,
    databaseId = databaseId))
  
  return(statTables)
  
  
}
### Factory

#' Create references to inclusion rules analysis per cohort
#'
#' Creates targets of inclusion rules for each cohort in the project
#'
#' @param cohortsToCreate A dataframe with one row per cohort and the following columns: cohortId, cohortName, cohortJsonPath
#' @param executionSettings An object containing all information of the database connection created from config file
#' @return One target for each generated cohort with names cohortInclusionRules_{id}
#' @export
tar_cohort_inclusion_rules <- function(cohortsToCreate,
                                       executionSettings) {
  
  #extract out all execution settings
  connectionDetails <- executionSettings$connectionDetails
  databaseId <- executionSettings$databaseId
  
  #create tibble to track generated cohorts
  nn <- 1:nrow(cohortsToCreate)
  iter <- tibble::tibble('generatedCohort' = rlang::syms(paste("generatedCohort", nn, sep ="_")),
                         'cohortId' = nn)
  
  #missing step that extracts the names of the inclusion rules from the cohort definitions
  #see CohortGeneratro::insertInclusionRuleNames
  #TODO add this function in different 
  list(
    tarchetypes::tar_map(values = iter, 
                         names = "cohortId",
                         tar_target_raw("cohortInclusionRules", 
                                        substitute(
                                          cohort_diagnostics_inclusion_rules(
                                            connectionDetails = connectionDetails,
                                            generatedCohort = generatedCohort,
                                            databaseId = databaseId)
                                        )
                         )
    )
  )
  
}

#######################################
#Time Series---------------------------
#######################################

#function

#' Time Series analysis for cohort diagnostics
#' 
#' Time series stratified by age group and gender in cohort diagnostics
#'
#' @param connectionDetails ConnectionDetails
#' @param cdmDatabaseSchema (character) Schema where the CDM data lives in the database
#' @param tempEmulationSchema the temporary table schema used in certain databases
#' @param generatedCohort dependency object of generated cohort class that tracks cohort used in incidence
#' analysis
#' @param observationPeriodRange an object stating the start and end of database observation
#' @return A dataframe with incidence calculations with several combinations
#' @export
cohort_diagnostics_time_series <- function(connectionDetails,
                                           cdmDatabaseSchema = config::get("cdmDatabaseSchema"),
                                           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                           generatedCohort,
                                           observationPeriodRange = NULL) {
  
  cohortDatabaseSchema <- generatedCohort$cohortTableRef$cohortDatabaseSchema
  cohortTable <- generatedCohort$cohortTableRef$cohortTableNames$cohortTable
  cohortId <- generatedCohort$cohort_id
  
  if (is.null(observationPeriodRange)) {
    observationPeriodRange <- data.frame(
      observationPeriodMinDate = as.Date("1980-01-01"),
      observationPeriodMaxDate = as.Date(Sys.Date())
    )
  }
  
  timeSeries <- CohortDiagnostics::runCohortTimeSeriesDiagnostics(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    runCohortTimeSeries = TRUE,
    runDataSourceTimeSeries = FALSE,
    timeSeriesMinDate = observationPeriodRange$observationPeriodMinDate,
    timeSeriesMaxDate = observationPeriodRange$observationPeriodMaxDate,
    stratifyByGender = TRUE,
    stratifyByAgeGroup = TRUE,
    cohortIds = cohortId
  )
  
  return(timeSeries)
}

#factory

#' Creates targets of time series for each cohort in the project
#'
#' @param cohortsToCreate A dataframe with one row per cohort and the following columns: cohortId, cohortName, cohortJsonPath
#' @param executionSettings An object containing all information of the database connection created from config file
#' @param observationPeriodRange the result of diagnostics_database_observation_period
#' @return One target for each generated cohort with names cohortTimeSeries_{id}
#' @export
tar_cohort_time_series <- function(cohortsToCreate,
                                   executionSettings,
                                   observationPeriodRange = NULL) {
  
  #extract out all execution settings
  connectionDetails <- executionSettings$connectionDetails
  cdmDatabaseSchema <- executionSettings$cdmDatabaseSchema
  databaseId <- executionSettings$databaseId
  
  
  nn <- 1:nrow(cohortsToCreate)
  
  iter <- tibble::tibble('generatedCohort' = rlang::syms(paste("generatedCohort", nn, sep ="_")),
                         'cohortId' = nn)
  
  
  list(
    tarchetypes::tar_map(values = iter, 
                         names = "cohortId",
                         tar_target_raw("cohortTimeSeries", 
                                        substitute(
                                          cohort_diagnostics_time_series(
                                            connectionDetails = connectionDetails,
                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                            generatedCohort = generatedCohort,
                                            observationPeriodRange = observationPeriodRange)
                                        )
                         )
    )
  )
  
}



#######################################
#cohort Relationship-------------------
#######################################

#' Define temporal covariates. Gives default settings
#' @export
define_temporal_covariates <- function() {
  temporalCovariates <-  FeatureExtraction::createTemporalCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useDemographicsAgeGroup = TRUE,
    useDemographicsRace = TRUE,
    useDemographicsEthnicity = TRUE,
    useDemographicsIndexYear = TRUE,
    useDemographicsIndexMonth = TRUE,
    useDemographicsIndexYearMonth = TRUE,
    useDemographicsPriorObservationTime = TRUE,
    useDemographicsPostObservationTime = TRUE,
    useDemographicsTimeInCohort = TRUE,
    useConditionOccurrence = TRUE,
    useProcedureOccurrence = TRUE,
    useDrugEraStart = TRUE,
    useMeasurement = TRUE,
    useConditionEraStart = TRUE,
    useConditionEraOverlap = TRUE,
    useConditionEraGroupOverlap = TRUE,
    useDrugEraGroupOverlap = TRUE,
    useObservation = TRUE,
    useDeviceExposure = TRUE,
    useCharlsonIndex = TRUE,
    useDcsi = TRUE,
    useChads2 = TRUE,
    useChads2Vasc = TRUE,
    temporalStartDays = c(
      # components displayed in cohort characterization
      -9999, # anytime prior
      -365, # long term prior
      -180, # medium term prior
      -30, # short term prior
      
      # components displayed in temporal characterization
      -365, # one year prior to -31
      -30, # 30 day prior not including day 0
      0, # index date only
      1, # 1 day after to day 30
      31,
      -9999 # Any time prior to any time future
    ),
    temporalEndDays = c(
      0, # anytime prior
      0, # long term prior
      0, # medium term prior
      0, # short term prior
      
      # components displayed in temporal characterization
      -31, # one year prior to -31
      -1, # 30 day prior not including day 0
      0, # index date only
      30, # 1 day after to day 30
      365,
      9999 # Any time prior to any time future
    )
  )
  return(temporalCovariates)
  
}


#function

#' cohort relationship analysis for cohort diagnostics
#'
#' @param connectionDetails ConnectionDetails
#' @param cdmDatabaseSchema (character) Schema where the CDM data lives in the database
#' @param generatedCohort dependency object of generated cohort class that tracks cohort used in incidence
#' analysis
#' @param temporalCovariateSettings temporal covariate settings used for analysis
#' @export
cohort_diagnostics_cohort_relationship <- function(connectionDetails,
                                                   cdmDatabaseSchema = config::get("cdmDatabaseSchema"),
                                                   targetGeneratedCohort,
                                                   comparatorGeneratedCohorts,
                                                   temporalCovariateSettings) {
  
  cohortDatabaseSchema <- targetGeneratedCohort$cohortTableRef$cohortDatabaseSchema
  cohortTable <- targetGeneratedCohort$cohortTableRef$cohortTableNames$cohortTable
  
  #TODO error logic if target and comparator cohort ids dont match
  targetCohortId <- targetGeneratedCohort$cohort_id
  comparatorCohortIds <- purrr::map_dbl(comparatorGeneratedCohorts, ~.x$cohort_id) %>%
    as.integer()
 
  relationshipDays <- tibble::tibble(
    startDay = temporalCovariateSettings$temporalStartDays,
    endDay = temporalCovariateSettings$temporalEndDays
  )
  
  cohortRelationship <- CohortDiagnostics::runCohortRelationshipDiagnostics(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    targetCohortIds = targetCohortId,
    comparatorCohortIds = comparatorCohortIds,
    relationshipDays = relationshipDays,
    observationPeriodRelationship = TRUE
  )
  
  return(cohortRelationship)
}




#' This is the target factory for the cohort relationships in cohort diagnostics
#'
#' @param cohortsToCreate a dataframe with the cohorts to create
#' @param temporalCovariateSettings a list of settings for cohort relationship analysis
#' @param executionSettings An object containing all information of the database connection created from config file
#' @export
tar_cohort_relationship <- function(cohortsToCreate,
                                    temporalCovariateSettings,
                                    executionSettings) {
  
  #extract out all execution settings
  connectionDetails <- executionSettings$connectionDetails
  cdmDatabaseSchema <- executionSettings$cdmDatabaseSchema
  databaseId <- executionSettings$databaseId
  studyName <- executionSettings$studyName

  nn <- 1:nrow(cohortsToCreate)
  `%notin%` <- Negate("%in%")
  comparatorCohortIds <- purrr::map(nn, ~nn[nn %notin% .x]) %>%
    purrr::map(~rlang::syms(paste("generatedCohort", .x, sep ="_"))) 
    
  
  
  iter <- tibble::tibble(
    'targetGeneratedCohort' = rlang::syms(paste("generatedCohort", nn, sep ="_")),
    'comparatorGeneratedCohorts' = comparatorCohortIds,
    'cohortId' = nn)
  
  list(
    tarchetypes::tar_map(values = iter, 
                         names = "cohortId",
                         tar_target_raw("cohortRelationship", 
                                        substitute(
                                          cohort_diagnostics_cohort_relationship(
                                            connectionDetails = connectionDetails,
                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                            targetGeneratedCohort = targetGeneratedCohort,
                                            comparatorGeneratedCohorts = comparatorGeneratedCohorts,
                                            temporalCovariateSettings = temporalCovariateSettings)
                                        )
                         )
    )
  )
  
  
}

##########################################
#Temporal COhort Characterization---------
##########################################

#function

# cohort_diagnostics_temporal_characterization <- function(connectionDetails,
#                                                    cdmDatabaseSchema = config::get("cdmDatabaseSchema"),
#                                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
#                                                    generatedCohort,
#                                                    temporalCovariateSettings) {
#   
#   cohortDatabaseSchema <- generatedCohort$cohortTableRef$cohortDatabaseSchema
#   cohortTable <- generatedCohort$cohortTableRef$cohortTableNames$cohortTable
#   cohortId <- generatedCohort$cohort_id
#   
#   
#   
#   characteristics <- CohortDiagnostics:::getCohortCharacteristics(
#     connectionDetails = connectionDetails,
#     cdmDatabaseSchema = cdmDatabaseSchema,
#     tempEmulationSchema = tempEmulationSchema,
#     cohortDatabaseSchema = cohortDatabaseSchema,
#     cohortTable = cohortTable,
#     cohortIds = cohortId,
#     covariateSettings = temporalCovariateSettings,
#     cdmVersion = 5L
#   )
#   
#   return(characteristics)
# }





# tar_temporal_cohort_characterization <- function(cohortsToCreate,
#                                                  temporalCovariateSettings,
#                                                  executionSettings) {
#   
#   #extract out all execution settings
#   connectionDetails <- executionSettings$connectionDetails
#   cdmDatabaseSchema <- executionSettings$cdmDatabaseSchema
#   vocabularyDatabaseSchema <- executionSettings$vocabularyDatabaseSchema
#   databaseId <- executionSettings$databaseId
#   
#   
#   nn <- 1:nrow(cohortsToCreate)
#   
#   iter <- tibble::tibble('generatedCohort' = rlang::syms(paste("generatedCohort", nn, sep ="_")),
#                          'cohortId' = nn)
#   
#   
#   list(
#     tarchetypes::tar_map(values = iter, 
#                          names = "cohortId",
#                          tar_target_raw("cohortTemporalCharacterization", 
#                                         substitute(
#                                           cohort_diagnostics_temporal_characterization(
#                                             connectionDetails = connectionDetails,
#                                             cdmDatabaseSchema = cdmDatabaseSchema,
#                                             generatedCohort = generatedCohort,
#                                             temporalCovariateSettings = temporalCovariateSettings)
#                                         )
#                          )
#     )
#   )
#   
# }

#' This is the target factory for cohort diagnostics
#'
#' @param cohortsToCreate a dataframe with the cohorts to create
#' @param executionSettings An object containing all information of the database connection created from config file
#' @param incidenceAnalysisSettings a dataframe where each row is a different combination of anlayis
#' @param temporalCovariateSettings a list of settings for cohort relationship analysis
#' @export
tar_cohort_diagnostics <- function(cohortsToCreate,
                                   executionSettings,
                                   incidenceAnalysisSettings,
                                   temporalCovariateSettings){
  list(
    tar_cohort_inclusion_rules(cohortsToCreate,
                               executionSettings),
    tar_cohort_incidence(cohortsToCreate,
                         incidenceAnalysisSettings,
                         executionSettings),
    tar_cohort_time_series(cohortsToCreate,
                           executionSettings),
    tar_cohort_relationship(cohortsToCreate,
                            temporalCovariateSettings,
                            executionSettings)#,
    # tar_temporal_cohort_characterization(cohortsToCreate,
    #                                      temporalCovariateSettings,
    #                                      executionSettings)
  )
}
