SITE_METADATA_URL <- paste0(
    'http://www.bom.gov.au/jsp/ncc/cdio/weatherStationDirectory/d',
    '?p_display_type=ajaxStnListing&p_nccObsCode=136&p_stnNum=%1$s&p_radius=0.01'
)

# HACK(mike): can't find a way to use namespace:: syntax with an infix operator, so import it this way
`%>%` <- rvest::`%>%`

#' Get the site data (ie, metadata) from the BOM website
#' @export
bomdata_get_site_raw <- function(site_number, url = SITE_METADATA_URL) {
    site_data_url <- sprintf(url, site_number)

    futile.logger::flog.debug('Downloading site data from %s', site_data_url, name = 'bomdata.site')
    site_meta_page <- xml2::read_html(site_data_url)
    tables <- site_meta_page %>% rvest::html_node('table')
    if (is.na(xml2::xml_type(tables))) {
        futile.logger::flog.warn('No table present on %s', site_data_url, name = 'bomdata.site')
        return(NULL)
    }
    site_meta <- tables %>% rvest::html_table(fill = TRUE)

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

#' @export
bomdata_get_site_numbers_in_region <- function(region_name) {
    stopifnot(region_name %in% REGION_NAMES)
    site_list_url <- sprintf(REGION_LIST_URL, region_name)
    futile.logger::flog.debug('Downloading list of sites from %s', site_list_url, name = 'bomdata.site')
    lines <- readLines(site_list_url)
    n_lines <- length(lines)
    lines <- lines[-c(1 : 4, (n_lines - 5) : n_lines)]
    numbers <- read.fwf(textConnection(paste(lines, collapse = '\n')), widths = c(7))
    return(numbers[, 1])
}

#' @export
bomdata_load_site <- function(db_connection, site_number = NULL, site_data = NULL, ...) {
    if (is.null(site_data)) {
        site_data <- bomdata_get_site_raw(site_number, ...)
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

#' @export
bomdata_get_site <- function(db_connection, site_number) {
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
