SITE_METADATA_URL <- paste0(
  'http://www.bom.gov.au/jsp/ncc/cdio/weatherStationDirectory/d',
  '?p_display_type=ajaxStnListing&p_nccObsCode=%2$d&p_stnNum=%1$s&p_radius=0.01'
)
EMPTY_SITE_DATA_FRAME <- data.frame(
  number = numeric(),
  name = character(),
  latitude = numeric(),
  longitude = numeric(),
  elevation = numeric(),
  p_c = numeric(),
  type = character()
)

#' Get the site data (ie, metadata) from the BOM website as a data.frame with a
#' single row.
#' @param site_numbers The site number to retrieve.
#' @param type Which observation type to download metadata for. If the site has
#' no historical data for this type, returns a data frame with zero rows.
#' @export
download_site <- function(
  site_numbers,
  type = c('rainfall', 'max_temperature', 'min_temperature', 'solar_exposure')
) {
  type <- match.arg(type)

  do.call(rbind, lapply(site_numbers, function(site_number) {
    site_data_url <- sprintf(
      SITE_METADATA_URL,
      site_number,
      .get_ncc_obs_code(type)
    )

    flog.debug(
      'Downloading site data from %s', site_data_url, name = 'bomdata.site'
    )
    site_meta_page <- xml2::read_html(site_data_url)
    tables <- rvest::html_node(site_meta_page, 'table')
    if (is.na(xml2::xml_type(tables))) {
      futile.logger::flog.warn(
        'No table present on %s', site_data_url, name = 'bomdata.site'
      )
      return(EMPTY_SITE_DATA_FRAME)
    }
    site_meta <- rvest::html_table(tables, fill = TRUE)

    # Ensure we get only the chosen site, and pick the right columns
    site_meta <- site_meta[
      site_meta$Station == site_number,
      c(2, 3, 4, 5, 6, 11)
    ]
    if (nrow(site_meta) == 0) {
      futile.logger::flog.warn(
        'Site %s not present on %s', site_number, site_data_url,
        name = 'bomdata.site'
      )
      return(EMPTY_SITE_DATA_FRAME)
    }
    site_meta[1, 1] <- site_number
    colnames(site_meta) <- c(
      'number', 'name', 'latitude', 'longitude', 'elevation', 'p_c'
    )
    site_meta$type <- type

    site_meta
  }))
}

REGION_LIST_URL <- (
  'http://www.bom.gov.au/climate/data/lists_by_element/alpha%1$s_%2$d.txt'
)

#' Get a vector of site numbers in a specified region.
#'
#' Get a vector of site numbers in a specified region.
#' @param region_name One of 'AUS', 'WA', 'SA', 'VIC', 'NSW', 'NT', 'QLD',
#' 'TAS', 'ANT'.
#' @param type The observation for which to list sites
#' @export
get_site_numbers_in_region <- function(
  region_name = c('AUS', 'WA', 'SA', 'VIC', 'NSW', 'NT', 'QLD', 'TAS', 'ANT'),
  type = c('rainfall', 'max_temperature', 'min_temperature', 'solar_exposure')
) {
  region_name <- match.arg(region_name)
  type <- match.arg(type)
  site_list_url <- sprintf(
    REGION_LIST_URL,
    region_name,
    .get_ncc_obs_code(type)
  )
  flog.debug(
    'Downloading list of sites from %s', site_list_url, name = 'bomdata.site'
  )
  lines <- readLines(site_list_url)
  n_lines <- length(lines)
  lines <- lines[-c(1 : 4, (n_lines - 5) : n_lines)]
  numbers <- utils::read.fwf(
    textConnection(paste(lines, collapse = '\n')),
    widths = c(7)
  )
  return(numbers[, 1])
}

#' Retrieve and load site metadata into a database.
#'
#' Retrieve and load site metadata into a database.
#' @param db_connection The database connection to an initialised bomdata
#' database.
#' @param site_number The site number to load. Ignored if \code{site_data} is
#' not null.
#' @param site_data Site metadata retrieved via \code{download_site}.
#' @param ... Passed to \code{download_site}.
#' @export
add_site <- function(
  db_connection, site_number = NULL, site_data = NULL, ...
) {
  if (missing(site_data)) {
    site_data <- download_site(site_number, ...)
  }

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
    site_data[, c('number', 'name', 'latitude', 'longitude', 'elevation')]
  )

  DBI::dbExecute(
    db_connection,
    '
      INSERT OR REPLACE INTO bom_site_p_c VALUES (
        :site_number,
        :type,
        :p_c
      );
    ',
    data.frame(
      site_number = site_data$number,
      type = site_data$type,
      p_c = site_data$p_c,
      stringsAsFactors = FALSE
    )
  )

  return(TRUE)
}

#' Gets the site metadata as a single-row data.frame from an existing database
#'
#' Gets the site metadata as a single-row data.frame from an existing database
#' @param db_connection The database connection to an initialised bomdata
#' database.
#' @param site_number The site number to retrieve.
#' @export
get_site <- function(db_connection, site_number) {
  DBI::dbGetQuery(
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

.get_site_p_c <- function(
  db_connection, site_number,
  type = c('rainfall', 'max_temperature', 'min_temperature', 'solar_exposure')
) {
  type <- match.arg(type)
  DBI::dbGetQuery(
    db_connection,
    '
      SELECT
        p_c
      FROM
        bom_site_p_c
      WHERE
        site_number = :site_number
        AND type = :type
    ',
    data.frame(site_number = site_number, type = type, stringsAsFactors = FALSE)
  )[, 1]
}
