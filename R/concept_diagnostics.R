#concept set diagnostics


runConceptSetDiagnostics <- function(executionSettings,
                                     tempEmulationSchema = NULL,
                                     generatedCohorts,
                                     minCellCount = 5L) {
  
  connectionDetails <- executionSettings$connectionDetails
  cdmDatabaseSchema <- executionSettings$cdmDatabaseSchema
  vocabularyDatabaseSchema <- executionSettings$vocabularyDatabaseSchema
  cohortDatabaseSchema <- executionSettings$resultsDatabaseSchema
  cohortTable <- executionSettings$cohortTableName
  databaseId <- executionSettings$databaseId
  studyName <- executionSettings$studyName
  
  
  suppressMessages(connection <- DatabaseConnector::connect(connectionDetails))
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  concept_sets <- get_concept_sets(connection = connection,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                                   tempEmulationSchema = tempEmulationSchema,
                                   generatedCohorts = generatedCohorts)
  
  
  included_source_concept <- concept_diagnostics_included_source_concepts(connection = connection,
                                                                          concept_sets = concept_sets,
                                                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                                                          tempEmulationSchema = tempEmulationSchema,
                                                                          minCellCount = 5L,
                                                                          databaseId = databaseId)
  
  index_event_breakdown <- concept_diagnostics_index_event_breakdown(connection = connection,
                                                                     concept_sets = concept_sets,
                                                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                                                     tempEmulationSchema = tempEmulationSchema,
                                                                     minCellCount = 5L,
                                                                     databaseId = databaseId)
  
  orphan_concepts <- concept_diagnostics_orphan_concepts(connection = connection,
                                                         concept_sets = concept_sets,
                                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                                         tempEmulationSchema = tempEmulationSchema,
                                                         minCellCount = 5L,
                                                         databaseId = databaseId,
                                                         studyName = studyName)
  
  concept_set_diagnostics <- list(
    included_source_concept = included_source_concept,
    index_event_breakdown = index_event_breakdown,
    orphan_concepts = orphan_concepts
  )
  
  return(concept_set_diagnostics)
  
}







get_concept_sets <- function(connection,
                                cdmDatabaseSchema,
                                vocabularyDatabaseSchema,
                                tempEmulationSchema = NULL,
                                generatedCohorts) {
  #get concept sets
  cohorts <- purrr::map(generatedCohorts, ~.x$cohort_definition) %>%
    Capr::createCohortDataframe() %>%
    dplyr::select(cohortId, cohortName, json, sql) 
  
  conceptSets <- cohorts %>%
    CohortDiagnostics:::combineConceptSetsFromCohorts()
  
  uniqueConceptSets <-
    conceptSets[!duplicated(conceptSets$uniqueConceptSetId), ] %>%
    dplyr::select(-.data$cohortId, -.data$conceptSetId)
  
  #turn this into permanent table
  CohortDiagnostics:::instantiateUniqueConceptSets(
    uniqueConceptSets = uniqueConceptSets,
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    conceptSetsTable = "#inst_concept_sets"
  )
  
  concept_sets <- list(
    cohorts = cohorts,
    conceptSets = conceptSets,
    uniqueConceptSets = uniqueConceptSets
  )
  
  
  return(concept_sets)
}


concept_diagnostics_included_source_concepts <- function(connection,
                                                         concept_sets,
                                                         cdmDatabaseSchema,
                                                         tempEmulationSchema,
                                                         minCellCount = 5L,
                                                         databaseId) {

  #get concept sets
  conceptSets <- concept_sets$conceptSets
  
  
  sql <- SqlRender::loadRenderTranslateSql(
    "CohortSourceCodes.sql",
    packageName = "CohortDiagnostics",
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    cdm_database_schema = cdmDatabaseSchema,
    instantiated_concept_sets = "#inst_concept_sets",
    include_source_concept_table = "#inc_src_concepts",
    by_month = FALSE
  )
  DatabaseConnector::executeSql(connection = connection, sql = sql)
  counts <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @include_source_concept_table;",
      include_source_concept_table = "#inc_src_concepts",
      tempEmulationSchema = tempEmulationSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
    tidyr::tibble() %>%
    dplyr::rename(uniqueConceptSetId = .data$conceptSetId) %>%
    dplyr::inner_join(
      conceptSets %>% dplyr::select(
        .data$uniqueConceptSetId,
        .data$cohortId,
        .data$conceptSetId
      ),
      by = "uniqueConceptSetId"
    ) %>%
    dplyr::select(-.data$uniqueConceptSetId) %>%
    dplyr::mutate(databaseId = !!databaseId) %>%
    dplyr::relocate(
      .data$databaseId,
      .data$cohortId,
      .data$conceptSetId,
      .data$conceptId
    ) %>%
    dplyr::distinct() %>%
    dplyr::group_by(
      .data$databaseId,
      .data$cohortId,
      .data$conceptSetId,
      .data$conceptId,
      .data$sourceConceptId
    ) %>%
    dplyr::summarise(
      conceptCount = max(.data$conceptCount),
      conceptSubjects = max(.data$conceptSubjects)
    ) %>%
    dplyr::ungroup()
  
  included_source_concept <- CohortDiagnostics:::makeDataExportable(
    x = counts,
    tableName = "included_source_concept",
    minCellCount = minCellCount,
    databaseId = databaseId
  )
    
    sql <-
      "TRUNCATE TABLE @include_source_concept_table;\nDROP TABLE @include_source_concept_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      include_source_concept_table = "#inc_src_concepts",
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  
  return(included_source_concept)
}




# TODO add cohorts into this function
concept_diagnostics_index_event_breakdown <- function(connection,
                                                      concept_sets,
                                                      cdmDatabaseSchema,
                                                      cohortDatabaseSchema,
                                                      vocabularyDatabaseSchmea,
                                                      tempEmulationSchema,
                                                      minCellCount = 5L,
                                                      databaseId) {

  cohorts <- concept_sets$cohorts
  
  domains <-
    readr::read_csv(
      system.file("csv", "domains.csv", package = "CohortDiagnostics"),
      col_types = readr::cols(),
      guess_max = min(1e7)
    )
  
  runBreakdownIndexEvents <- function(cohort) {
    ParallelLogger::logInfo(
      "- Breaking down index events for cohort '",
      cohort$cohortName,
      "'"
    )
    
    cohortDefinition <-
      RJSONIO::fromJSON(cohort$json, digits = 23)
    primaryCodesetIds <-
      lapply(
        cohortDefinition$PrimaryCriteria$CriteriaList,
        CohortDiagnostics:::getCodeSetIds
      ) %>%
      dplyr::bind_rows()
    if (nrow(primaryCodesetIds) == 0) {
      warning(
        "No primary event criteria concept sets found for cohort id: ",
        cohort$cohortId
      )
      return(tidyr::tibble())
    }
    primaryCodesetIds <- primaryCodesetIds %>% dplyr::filter(.data$domain %in%
                                                               c(domains$domain %>% unique()))
    if (nrow(primaryCodesetIds) == 0) {
      warning(
        "Primary event criteria concept sets found for cohort id: ",
        cohort$cohortId, " but,", "\nnone of the concept sets belong to the supported domains.",
        "\nThe supported domains are:\n", paste(domains$domain,
                                                collapse = ", "
        )
      )
      return(tidyr::tibble())
    }
    primaryCodesetIds <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% cohort$cohortId) %>%
      dplyr::select(codeSetIds = .data$conceptSetId, .data$uniqueConceptSetId) %>%
      dplyr::inner_join(primaryCodesetIds, by = "codeSetIds")
    
    pasteIds <- function(row) {
      return(dplyr::tibble(
        domain = row$domain[1],
        uniqueConceptSetId = paste(row$uniqueConceptSetId, collapse = ", ")
      ))
    }
    primaryCodesetIds <-
      lapply(
        split(primaryCodesetIds, primaryCodesetIds$domain),
        pasteIds
      )
    primaryCodesetIds <- dplyr::bind_rows(primaryCodesetIds)
    
    
    getCounts <- function(row) {
      domain <- domains[domains$domain == row$domain, ]
      sql <-
        SqlRender::loadRenderTranslateSql(
          "CohortEntryBreakdown.sql",
          packageName = "CohortDiagnostics",
          dbms = connection@dbms,
          tempEmulationSchema = tempEmulationSchema,
          cdm_database_schema = cdmDatabaseSchema,
          vocabulary_database_schema = vocabularyDatabaseSchema,
          cohort_database_schema = cohortDatabaseSchema,
          cohort_table = cohortTable,
          cohort_id = cohort$cohortId,
          domain_table = domain$domainTable,
          domain_start_date = domain$domainStartDate,
          domain_concept_id = domain$domainConceptId,
          domain_source_concept_id = domain$domainSourceConceptId,
          use_source_concept_id = !(is.na(domain$domainSourceConceptId) | is.null(domain$domainSourceConceptId)),
          primary_codeset_ids = row$uniqueConceptSetId,
          concept_set_table = "#inst_concept_sets",
          store = TRUE,
          store_table = "#breakdown"
        )
      
      DatabaseConnector::executeSql(
        connection = connection,
        sql = sql,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
      sql <- "SELECT * FROM @store_table;"
      counts <-
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          store_table = "#breakdown",
          snakeCaseToCamelCase = TRUE
        ) %>%
        tidyr::tibble()
      
      sql <-
        "TRUNCATE TABLE @store_table;\nDROP TABLE @store_table;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        tempEmulationSchema = tempEmulationSchema,
        store_table = "#breakdown",
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
      return(counts)
    }
    
    counts <-
      lapply(split(primaryCodesetIds, 1:nrow(primaryCodesetIds)), getCounts) %>%
      dplyr::bind_rows() %>%
      dplyr::arrange(.data$conceptCount)
    
    if (nrow(counts) > 0) {
      counts$cohortId <- cohort$cohortId
    } else {
      ParallelLogger::logInfo(
        "Index event breakdown results were not returned for: ",
        cohort$cohortId
      )
      return(dplyr::tibble())
    }
    return(counts)
  }
  
  
  data <-
    lapply(
      split(cohorts, cohorts$cohortId),
      runBreakdownIndexEvents
    )
  data <- dplyr::bind_rows(data)
  if (nrow(data) > 0) {
    data <- data %>%
      dplyr::mutate(databaseId = !!databaseId)
    data <-
      CohortDiagnostics:::enforceMinCellValue(data, "conceptCount", minCellCount)
    if ("subjectCount" %in% colnames(data)) {
      data <-
        CohortDiagnostics:::enforceMinCellValue(data, "subjectCount", minCellCount)
    }
  }
  
  data_index_event <- CohortDiagnostics:::makeDataExportable(
    x = data,
    tableName = "index_event_breakdown",
    minCellCount = minCellCount,
    databaseId = databaseId
  )
  return(data_index_event)
}

concept_diagnostics_orphan_concepts <- function(connection,
                                                concept_sets,
                                                cdmDatabaseSchema,
                                                tempEmulationSchema,
                                                minCellCount = 5L,
                                                databaseId,
                                                studyName) {
  
  
  uniqueConceptSets <- concept_sets$uniqueConceptSets
  conceptCountsTable <- paste0("concept_counts", "_", studyName)
  
  #create concept counts table
  CohortDiagnostics:::createConceptCountsTable(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    conceptCountsDatabaseSchema = cohortDatabaseSchema,
    conceptCountsTable = conceptCountsTable,
    conceptCountsTableIsTemp = FALSE
  )
  
  # [OPTIMIZATION idea] can we modify the sql to do this for all uniqueConceptSetId in one query using group by?
  data <- list()
  for (i in (1:nrow(uniqueConceptSets))) {
    conceptSet <- uniqueConceptSets[i, ]
    ParallelLogger::logInfo(
      "- Finding orphan concepts for concept set '",
      conceptSet$conceptSetName,
      "'"
    )
    data[[i]] <- CohortDiagnostics:::.findOrphanConcepts(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      useCodesetTable = TRUE,
      codesetId = conceptSet$uniqueConceptSetId,
      conceptCountsDatabaseSchema = cdmDatabaseSchema,
      conceptCountsTable = conceptCountsTable,
      conceptCountsTableIsTemp = FALSE,
      instantiatedCodeSets = "#inst_concept_sets",
      orphanConceptTable = "#orphan_concepts"
    )
    
    sql <-
      "TRUNCATE TABLE @orphan_concept_table;\nDROP TABLE @orphan_concept_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      orphan_concept_table = "#orphan_concepts",
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  data <- dplyr::bind_rows(data) %>%
    dplyr::distinct() %>%
    dplyr::rename(uniqueConceptSetId = .data$codesetId) %>%
    dplyr::inner_join(
      conceptSets %>%
        dplyr::select(
          .data$uniqueConceptSetId,
          .data$cohortId,
          .data$conceptSetId
        ),
      by = "uniqueConceptSetId"
    ) %>%
    dplyr::select(-.data$uniqueConceptSetId) %>%
    dplyr::select(
      .data$cohortId,
      .data$conceptSetId,
      .data$conceptId,
      .data$conceptCount,
      .data$conceptSubjects
    ) %>%
    dplyr::group_by(
      .data$cohortId,
      .data$conceptSetId,
      .data$conceptId
    ) %>%
    dplyr::summarise(
      conceptCount = max(.data$conceptCount),
      conceptSubjects = max(.data$conceptSubjects)
    ) %>%
    dplyr::ungroup()
  data_orphan <- CohortDiagnostics:::makeDataExportable(
    x = data,
    tableName = "orphan_concept",
    minCellCount = minCellCount,
    databaseId = databaseId
  )
  return(data_orphan)
}