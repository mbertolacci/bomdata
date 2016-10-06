SITE_METADATA_URL <- paste0(
    'http://www.bom.gov.au/jsp/ncc/cdio/weatherStationDirectory/d',
    '?p_display_type=ajaxStnListing&p_nccObsCode=136&p_stnNum=%1$s&p_radius=0.01'
)

#' Get the site data (ie, metadata) from the BOM website as a data.frame with a single row.
#' @param site_number The site number to retrieve.
#' @param url The URL to retrieve the data from. The default should be correct; read the package source for details.
#' @export
get_site_raw <- function(site_number, url = SITE_METADATA_URL) {
    site_data_url <- sprintf(url, site_number)

    futile.logger::flog.debug('Downloading site data from %s', site_data_url, name = 'bomdata.site')
    site_meta_page <- xml2::read_html(site_data_url)
    tables <- rvest::html_node(site_meta_page, 'table')
    if (is.na(xml2::xml_type(tables))) {
        futile.logger::flog.warn('No table present on %s', site_data_url, name = 'bomdata.site')
        return(NULL)
    }
    site_meta <- rvest::html_table(tables, fill = TRUE)

    # Ensure we get only the chosen site, and pick the right columns
    site_meta <- site_meta[site_meta$Station == site_number, c(2, 3, 4, 5, 6, 11)]
    if (nrow(site_meta) == 0) {
        futile.logger::flog.warn('Site %s not present on %s', site_number, site_data_url, name = 'bomdata.site')
        return(NULL)
    }
    site_meta[1, 1] <- site_number
    colnames(site_meta) <- c('number', 'name', 'latitude', 'longitude', 'elevation', 'p_c')

    return(site_meta)
}

REGION_NAMES <- c('AUS', 'WA', 'SA', 'VIC', 'NSW', 'NT', 'QLD', 'TAS', 'ANT')
REGION_LIST_URL <- 'http://www.bom.gov.au/climate/data/lists_by_element/alpha%1$s_136.txt'

#' Get a vector of site numbers in a specified region.
#'
#' Get a vector of site numbers in a specified region.
#' @param region_name One of 'AUS', 'WA', 'SA', 'VIC', 'NSW', 'NT', 'QLD', 'TAS', 'ANT'.
#' @export
get_site_numbers_in_region <- function(region_name) {
    stopifnot(region_name %in% REGION_NAMES)
    site_list_url <- sprintf(REGION_LIST_URL, region_name)
    futile.logger::flog.debug('Downloading list of sites from %s', site_list_url, name = 'bomdata.site')
    lines <- readLines(site_list_url)
    n_lines <- length(lines)
    lines <- lines[-c(1 : 4, (n_lines - 5) : n_lines)]
    numbers <- utils::read.fwf(textConnection(paste(lines, collapse = '\n')), widths = c(7))
    return(numbers[, 1])
}

#' Retrieve and load site metadata into a database.
#'
#' Retrieve and load site metadata into a database.
#' @param db_connection The database connection to an initialised bomdata database.
#' @param site_number The site number to load. Ignored if \code{site_data} is not null.
#' @param site_data Site metadata retrieved via \code{get_site} or \code{get_site_raw}.
#' @param ... Passed to \code{get_site_raw}.
#' @export
load_site <- function(db_connection, site_number = NULL, site_data = NULL, ...) {
    if (is.null(site_data)) {
        site_data <- get_site_raw(site_number, ...)
    }

    RSQLite::dbGetPreparedQuery(
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

    return(TRUE)
}

#' Gets the site metadata as a single-row data.frame from an existing database
#'
#' Gets the site metadata as a single-row data.frame from an existing database
#' @param db_connection The database connection to an initialised bomdata database.
#' @param site_number The site number to retrieve.
#' @export
get_site <- function(db_connection, site_number) {
    RSQLite::dbGetPreparedQuery(
        db_connection,
        '
            SELECT
                *
            FROM
                bom_site
            WHERE
                number = :number
        ',
        data.frame(number = site_number)
    )
}
