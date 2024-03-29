% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/cohort_diagnostics.R
\name{tar_cohort_inclusion_rules}
\alias{tar_cohort_inclusion_rules}
\title{Create references to inclusion rules analysis per cohort}
\usage{
tar_cohort_inclusion_rules(cohortsToCreate, executionSettings)
}
\arguments{
\item{cohortsToCreate}{A dataframe with one row per cohort and the following columns: cohortId, cohortName, cohortJsonPath}

\item{executionSettings}{An object containing all information of the database connection created from config file}
}
\value{
One target for each generated cohort with names cohortInclusionRules_{id}
}
\description{
Creates targets of inclusion rules for each cohort in the project
}
