get_site_data <- function(site_number) {
  futile.logger::flog.debug(
    'Getting data for %d', site_number, name = 'bomdata.util'
  )
  site_data <- download_site(site_number)
  if (is.null(site_data)) {
    return(NULL)
  }
  rainfall_data <- download_rainfall(site_data)
  if (is.null(rainfall_data)) {
    return(NULL)
  }
  return(list(site_data = site_data, rainfall_data = rainfall_data))
}

#' Load many sites along with rainfall data into a database in parallel.
#'
#' Load many sites along with rainfall data into a database in parallel.
#' @param db_connection A connection to an initialised bomdata database.
#' @param site_numbers A vector of site numbers to load. Sites that already
#' exist within the database will be ignored.
#' @param n_parallel The number of parallel workers to run to download sites.
#' Only works in mclapply works on your machine.
#' @param batch_size The number of sites to download before committing their
#' data to the database.
#' @export
add_many_sites <- function(db_connection, site_numbers, n_parallel = 8,
                           batch_size = 40) {
  site_numbers <- strtoi(site_numbers)

  # Exclude site numbers that already exist
  existing_site_numbers <- DBI::dbGetQuery(
    db_connection, 'SELECT number FROM bom_site'
  )$number
  site_numbers <- site_numbers[! site_numbers %in% existing_site_numbers]

  # Strategy: split into batches of a given size, downloading the data for
  # each batch in parallel, then importing the batch in one go.
  batches <- split(site_numbers, ceiling(
    seq_along(site_numbers) / batch_size
  ))
  for (batch in batches) {
    batch_data <- parallel::mclapply(
      batch, get_site_data, mc.cores = n_parallel
    )

    futile.logger::flog.debug('Loading batch', name = 'bomdata.util')
    for (site in batch_data) {
      if (is.null(site)) {
        next
      }
      DBI::dbBegin(db_connection)
      add_site(db_connection, site_data = site$site_data)
      add_rainfall(
        db_connection,
        site_data = site$site_data,
        rainfall_data = site$rainfall_data
      )
      DBI::dbCommit(db_connection)
    }
  }
}

HIGH_QUALITY_RAINFALL_URL <- (
  'ftp://ftp.bom.gov.au/anon/home/ncc/www/change/HQdailyR/HQ_daily_prcp_txt.tar'
)

#' Load the high quality daily rainfall database.
#'
#' Load the high quality daily rainfall database.
#' @param db_connection A connection to an initialised bomdata database.
#' @param tar_filename Disk location of the an existing tar file
#' @param url Location to download tar file from, if tar_filename is NULL
#' @export
add_high_quality_rainfall <- function(db_connection, tar_filename = NULL,
                                      url = HIGH_QUALITY_RAINFALL_URL,
                                      quiet = FALSE) {
  if (is.null(tar_filename)) {
    futile.logger::flog.debug(
      'Downloading high quality rainfall database from %s', url,
      name = 'bomdata.util'
    )
    tar_filename <- tempfile()
    utils::download.file(url, tar_filename, quiet = quiet)
  }

  output_directory <- file.path(tempdir(), 'bomdata_hqdaily')

  futile.logger::flog.debug('Unpacking the tar', name = 'bomdata.util')
  utils::untar(tar_filename, exdir = output_directory)

  site_data <- utils::read.fwf(
    file.path(output_directory, 'HQDR_stations.txt'),
    widths = c(7, 7, 7, 7, 200),
    stringsAsFactors = FALSE
  )
  site_data$p_c <- -1
  colnames(site_data) <- c(
    'number', 'latitude', 'longitude', 'elevation', 'name', 'p_c'
  )
  DBI::dbExecute(
    db_connection,
    '
      INSERT OR REPLACE INTO bom_site VALUES (
        :number,
        :name,
        :latitude,
        :longitude,
        :elevation,
        :p_c
      );
    ',
    site_data
  )

  for (site_number in site_data$number) {
    futile.logger::flog.debug(
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
