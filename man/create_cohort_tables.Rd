% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/generate.R
\name{create_cohort_tables}
\alias{create_cohort_tables}
\title{Create cohort tables in a CDM database}
\usage{
create_cohort_tables(name, connectionDetails, cohortDatabaseSchema)
}
\arguments{
\item{name}{The name of the cohort table as a string. Cannot contain spaces or special characters.}

\item{connectionDetails}{ConnectionDetails used to connect to the database. Created by DatabaseConnecton::createConnectionDetails()}

\item{cohortDatabaseSchema}{Schema where the cohort tables will be created. Write access is required.}
}
\value{
An object of class cohortTableRef that acts as a reference to the cohort tables created by the function.
}
\description{
Create cohort tables in a CDM database
}
