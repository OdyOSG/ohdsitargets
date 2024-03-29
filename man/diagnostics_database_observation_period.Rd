% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/database_diagnostics.R
\name{diagnostics_database_observation_period}
\alias{diagnostics_database_observation_period}
\title{Get information about the observation period}
\usage{
diagnostics_database_observation_period(
  connectionDetails,
  cdmDatabaseSchema,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
)
}
\arguments{
\item{connectionDetails}{ConnectionDetails}

\item{cdmDatabaseSchema}{(character) Schema where the CDM data lives in the database}

\item{tempEmulationSchema}{the temporary table schema used in certain databases}
}
\value{
A tibble with information about the observation period
}
\description{
See the length of coverage for the database in calendar and person years
}
