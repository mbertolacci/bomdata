DAILY_ZIPPED_DATA_URL <- paste0(
  'http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av',
  '?p_display_type=dailyZippedDataFile&p_stn_num=%1$06d&p_nccObsCode=%2$d',
  '&p_c=%3$.f'
)

EMPTY_DAILY_CLIMATE_DATA_FRAME <- data.frame(
  date = character(),
  value = numeric(),
  days_measured = integer(),
  quality = integer()
)

#' Daily climate data for a site.
#'
#' Downloads daily climate data for site. This is a dataframe with 4 columns,
#' date', x, 'days_measured' and 'quality', where x is the type requested.
#' @param site_number Integer number of site for which to download data.
#' @param type Type of data to download.
#' @param p_c Internal code required by BOM for downloads to work. If not
#' provided, this function will find it.
#' @export
download_daily_climate_data <- function(
  site_number,
  type = c('rainfall', 'max_temperature', 'min_temperature', 'solar_exposure'),
  p_c = NULL
) {
  type <- match.arg(type)

  if (missing(p_c)) {
    p_c <- download_site(site_number, type)$p_c
  }
  url <- sprintf(
    DAILY_ZIPPED_DATA_URL,
    site_number,
    .get_ncc_obs_code(type),
    p_c
  )

  flog.debug(
    'Downloading site data zip from %s',
    url, name = 'bomdata.climate-data'
  )
  data_zip_temp <- tempfile()
  utils::download.file(url, data_zip_temp, quiet = TRUE)

  flog.debug('Unpacking the zip', name = 'bomdata.climate-data')
  data_csv <- unz(
    data_zip_temp,
    sprintf('%s_%06d_1800_Data.csv', .get_product_code(type), site_number)
  )
  # HACK(mgnb): want to know if we got an empty file (happens sometimes), so
  # read to lines first
  data_lines <- readLines(data_csv)
  close(data_csv)
  if (length(data_lines) == 0) {
    futile.logger::flog.warn(
      'No data present on %s for %s', url, site_number,
      name = 'bomdata.climate-data'
    )
    data <- EMPTY_DAILY_CLIMATE_DATA_FRAME
    colnames(data)[2] <- type
    return(data)
  }
  data <- utils::read.csv(text = paste0(data_lines, collapse = '\n'))

  if (type == 'solar_exposure') {
    data <- cbind(data[, 3 : 6], NA, NA)
  } else {
    data <- data[, 3 : 8]
  }
  colnames(data) <- c(
    'year', 'month', 'day', type, 'days_measured', 'quality'
  )
  data$date <- sprintf(
    '%d-%02d-%02d',
    strtoi(data$year),
    strtoi(data$month),
    strtoi(data$day)
  )

  data[, c(7, 4, 5, 6)]
}

#' Add daily climate data to a database.
#'
#' @param db_connection The database connection to an initialised bomdata
#' database.
#' @param site_number The site number to load. Ignored if \code{site_data} is
#' not null.
#' @param data A data frame retrieved using \code{download_daily_climate_data}.
#' Optional. If not provided, this function will download the data.
#' @param type Type of data to add.
#' @param ... Will be passed to \code{download_daily_climate_data}
#' @export
add_daily_climate_data <- function(
  db_connection,
  site_number,
  data,
  type = c('rainfall', 'max_temperature', 'min_temperature', 'solar_exposure'),
  ...
) {
  type <- match.arg(type)

  if (missing(data)) {
    p_c <- .get_site_p_c(db_connection, site_number, type)
    if (length(p_c) == 0) {
      add_site(db_connection, site_number, type = type)
      p_c <- .get_site_p_c(db_connection, site_number, type)
    }
    data <- download_daily_climate_data(
      site_number, type = type, p_c = p_c, ...
    )
  }

  if (nrow(data) == 0) return(invisible(NULL))
  data$site_number <- site_number
  data <- data[, c(5, 1, 2, 3, 4)]

  DBI::dbExecute(
    db_connection,
    sprintf('
      DELETE FROM
        bom_%s
      WHERE
        site_number = :site_number
    ', type),
    data.frame(site_number = site_number)
  )
  DBI::dbWriteTable(
    db_connection, sprintf('bom_%s', type), data, append = TRUE
  )

  invisible(TRUE)
}
