get_site_data <- function(site_number) {
    futile.logger::flog.debug('Getting data for %d', site_number, name = 'bomdata.util')
    site_data <- get_site_raw(site_number)
    if (is.null(site_data)) {
        return(NULL)
    }
    rainfall_data <- get_rainfall_raw(site_data)
    if (is.null(rainfall_data)) {
        return(NULL)
    }
    return(list(site_data = site_data, rainfall_data = rainfall_data))
}

#' Load many sites along with rainfall data into a database in parallel.
#'
#' Load many sites along with rainfall data into a database in parallel.
#' @param db_connection A connection to an initialised bomdata database.
#' @param site_numbers A vector of site numbers to load. Sites that already exist within the database will be ignored.
#' @param n_parallel The number of parallel workers to run to download sites. Only works in mclapply works on your
#' machine.
#' @param batch_size The number of sites to download before committing their data to the database.
#' @export
load_many_sites <- function(db_connection, site_numbers, n_parallel = 8, batch_size = 40) {
    site_numbers <- strtoi(site_numbers)

    # Exclude site numbers that already exist
    existing_site_numbers <- DBI::dbGetQuery(db_connection, 'SELECT number FROM bom_site')$number
    site_numbers <- site_numbers[! site_numbers %in% existing_site_numbers]

    # Strategy: split into batches of a given size, downloading the data for each batch in parallel, then importing
    # the batch in one go.
    batches <- split(site_numbers, ceiling(seq_along(site_numbers) / batch_size))
    for (batch in batches) {
        batch_data <- parallel::mclapply(batch, get_site_data, mc.cores = n_parallel)

        futile.logger::flog.debug('Loading batch', name = 'bomdata.util')
        for (site in batch_data) {
            if (is.null(site)) {
                next
            }
            DBI::dbBegin(db_connection)
            load_site(db_connection, site_data = site$site_data)
            load_rainfall(db_connection, site_data = site$site_data, rainfall_data = site$rainfall_data)
            DBI::dbCommit(db_connection)
        }
    }
}
