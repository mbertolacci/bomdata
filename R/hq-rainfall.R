HIGH_QUALITY_RAINFALL_URL <- (
  'ftp://ftp.bom.gov.au/anon/home/ncc/www/change/HQdailyR/HQ_daily_prcp_txt.tar'
)

#' Load the high quality daily rainfall database.
#'
#' Load the high quality daily rainfall database.
#' @param db_connection A connection to an initialised bomdata database.
#' @param tar_filename Disk location of the an existing tar file
#' @param url Location to download tar file from, if tar_filename is NULL
#' @param quiet Whether to show download progress
#' @export
add_high_quality_rainfall <- function(
  db_connection, tar_filename = NULL, url = HIGH_QUALITY_RAINFALL_URL,
  quiet = FALSE
) {
  if (is.null(tar_filename)) {
    flog.debug(
      'Downloading high quality rainfall database from %s', url,
      name = 'bomdata.util'
    )
    tar_filename <- tempfile()
    utils::download.file(url, tar_filename, quiet = quiet)
  }

  output_directory <- file.path(tempdir(), 'bomdata_hqdaily')

  flog.debug('Unpacking the tar', name = 'bomdata.util')
  utils::untar(tar_filename, exdir = output_directory)

  site_data <- utils::read.fwf(
    file.path(output_directory, 'HQDR_stations.txt'),
    widths = c(7, 7, 7, 7, 200),
    stringsAsFactors = FALSE
  )
  colnames(site_data) <- c(
    'number', 'latitude', 'longitude', 'elevation', 'name'
  )
  DBI::dbExecute(
    db_connection,
    '
      INSERT OR REPLACE INTO bom_site VALUES (
        :number,
        :name,
        :latitude,
        :longitude,
        :elevation
      );
    ',
    site_data
  )

  for (site_number in site_data$number) {
    flog.debug(
      'Loading data for %d', site_number,
      name = 'bomdata.util'
    )

    rainfall_z_filename <- file.path(
      output_directory,
      sprintf('prcphq.%06d.daily.txt.Z', site_number)
    )
    system2('uncompress', rainfall_z_filename)

    rainfall_filename <- file.path(
      output_directory,
      sprintf('prcphq.%06d.daily.txt', site_number)
    )

    raw_data <- utils::read.fwf(
      rainfall_filename,
      skip = 1,
      widths = c(9, 9),
      stringsAsFactors = FALSE
    )

    year <- floor(raw_data[, 1] / 10000)
    month <- floor(raw_data[, 1] %% 10000 / 100)
    day <- floor(raw_data[, 1] %% 100)

    data <- data.frame(
      site_number = site_number,
      date = sprintf('%d-%02d-%02d', year, month, day),
      rainfall = ifelse(raw_data[, 2] == 99999.9, NA, raw_data[, 2]),
      days_measured = 1,
      quality = 'Y'
    )
    DBI::dbWriteTable(db_connection, 'bom_rainfall', data, append = TRUE)
  }
}
