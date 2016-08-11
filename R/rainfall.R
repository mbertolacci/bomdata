RAINFALL_URL <- paste0(
    'http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av',
    '?p_display_type=dailyZippedDataFile&p_stn_num=%1$06d&p_nccObsCode=136',
    '&p_c=%2$.f'
)

#' Get the rainfall data for a site. This is a dataframe with 4 columns, 'date', 'rainfall', 'days_measured' and
#' 'quality'.
#' @param site_data Site metadata (as a data frame row) retrieved via \code{bomdata_get_site} or
#' \code{bomdata_get_site_raw}
#' @param url The URL to retrieve the data from. The default should be correct; read the package source for details.
#' @export
bomdata_get_rainfall_raw <- function(site_data, url = RAINFALL_URL) {
    rainfall_url <- sprintf(url, site_data$number, site_data$p_c)

    futile.logger::flog.debug('Downloading site rainfall zip from %s', rainfall_url, name = 'bomdata.rainfall')
    data_zip_temp <- tempfile()
    utils::download.file(rainfall_url, data_zip_temp, quiet = TRUE)

    futile.logger::flog.debug('Unpacking the zip', name = 'bomdata.rainfall')
    data_csv <- unz(
        data_zip_temp,
        sprintf('IDCJAC0009_%06d_1800_Data.csv', site_data$number)
    )
    # HACK(mgnb): want to know if we got an empty file (happens sometimes), so read to lines first
    data_lines <- readLines(data_csv)
    close(data_csv)
    if (length(data_lines) == 0) {
        futile.logger::flog.warn(
            'No data present on %s for %s', rainfall_url, site_data$number,
            name = 'bomdata.rainfall'
        )
        return(NULL)
    }
    data <- utils::read.csv(text = paste0(data_lines, collapse = '\n'))

    data <- data[, 3 : 8]
    colnames(data) <- c('year', 'month', 'day', 'rainfall', 'days_measured', 'quality')
    data$date <- sprintf(
        '%d-%02d-%02d',
        strtoi(data$year),
        strtoi(data$month),
        strtoi(data$day)
    )
    data <- data[, c(7, 4, 5, 6)]

    return(data)
}

#' Load the rainfall data for a site into the database. The site must already exist in the database.
#' @param db_connection The database connection to an initialised bomdata database.
#' @param site_number The site number to load. Ignored if \code{site_data} is not null.
#' @param site_data Site metadata retrieved via \code{bomdata_get_site} or \code{bomdata_get_site_raw}. Ignored if
#' \code{rainfall_data} is not null.
#' @param rainfall_data A data frame of rainfall data retrieved using \code{bomdata_get_rainfall_raw}.
#' @param ... Will be passed to \code{bomdata_get_rainfall_raw}
#' @export
bomdata_load_rainfall <- function(db_connection, site_number = NULL, site_data = NULL, rainfall_data = NULL, ...) {
    if (is.null(site_data)) {
        site_data <- bomdata_get_site(db_connection, site_number)
    }
    if (is.null(rainfall_data)) {
        rainfall_data <- bomdata_get_rainfall_raw(site_data, ...)
    }

    rainfall_data$site_number <- site_data$number[1]
    rainfall_data <- rainfall_data[, c(5, 1, 2, 3, 4)]

    futile.logger::flog.debug('Adding site data', name = 'bomdata.rainfall')
    return(DBI::dbWriteTable(db_connection, 'bom_rainfall', rainfall_data, append = TRUE))
}
