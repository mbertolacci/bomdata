#' Load many sites into a database in parallel.
#'
#' Load many sites along with rainfall data into a database in parallel.
#' @param db_connection A connection to an initialised bomdata database.
#' @param site_numbers A vector of site numbers to load. Sites that already
#' exist within the database will be ignored.
#' @param type Observation type to load.
#' @param n_parallel The number of parallel workers to run to download sites.
#' Only works in mclapply works on your machine.
#' @param batch_size The number of sites to download before committing their
#' data to the database.
#' @export
add_many_sites <- function(
  db_connection, site_numbers, type = c('rainfall', 'max_temperature'),
  n_parallel = 8, batch_size = 40
) {
  site_numbers <- strtoi(site_numbers)

  # Strategy: split into batches of a given size, downloading the data for
  # each batch in parallel, then importing the batch in one go.
  batches <- split(site_numbers, ceiling(
    seq_along(site_numbers) / batch_size
  ))
  for (batch in batches) {
    batch_data <- parallel::mclapply(
      batch,
      function(site_number) {
        flog.debug('Downloading %d', site_number, name = 'bomdata.add-many')
        site_data <- download_site(site_number, type)
        if (nrow(site_data) == 0) {
          return(NULL)
        }
        climate_data <- download_daily_climate_data(
          site_number,
          type,
          p_c = site_data$p_c
        )
        list(
          site_data = site_data,
          climate_data = climate_data
        )
      },
      mc.cores = n_parallel
    )

    flog.debug(
      'Adding batch to database', name = 'bomdata.add-many'
    )
    for (site in batch_data) {
      if (is.null(site)) {
        next
      }
      if (inherits(site, 'try-error')) {
        print(site)
        stop('Site had error')
      }
      DBI::dbBegin(db_connection)
      add_site(db_connection, site_data = site$site_data)
      add_daily_climate_data(
        db_connection,
        site$site_data$number,
        site$climate_data,
        type = type
      )
      DBI::dbCommit(db_connection)
    }
  }
}
