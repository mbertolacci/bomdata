% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/climate-data.R
\name{download_daily_climate_data}
\alias{download_daily_climate_data}
\title{Daily climate data for a site.}
\usage{
download_daily_climate_data(site_number, type = c("rainfall",
  "max_temperature", "min_temperature", "solar_exposure"), p_c = NULL)
}
\arguments{
\item{site_number}{Integer number of site for which to download data.}

\item{type}{Type of data to download.}

\item{p_c}{Internal code required by BOM for downloads to work. If not
provided, this function will find it.}
}
\description{
Downloads daily climate data for site. This is a dataframe with 4 columns,
date', x, 'days_measured' and 'quality', where x is the type requested.
}
