get_site_data <- function(site_number) {
    futile.logger::flog.debug('Getting data for %d', site_number, name = 'bomdata.util')
    site_data <- bomdata_get_site_raw(site_number)
    if (is.null(site_data)) {
        return(NULL)
    }
    rainfall_data <- bomdata_get_rainfall_raw(site_data)
    if (is.null(rainfall_data)) {
        return(NULL)
    }
    return(list(site_data = site_data, rainfall_data = rainfall_data))
}

#' @export
bomdata_load_many_sites <- function(db_connection, site_numbers, n_parallel = 8, batch_size = 40) {
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
            bomdata_load_site(db_connection, site_data = site$site_data)
            bomdata_load_rainfall(db_connection, site_data = site$site_data, rainfall_data = site$rainfall_data)
            DBI::dbCommit(db_connection)
        }
    }
}
