#' Initialise a bomdata database.
#'
#' Initialises a bomdata database. The database can have been initialised in
#' the past, so it's safe to call this again.
#' @param db_connection An RSQLite connection.
#' @export
initialise_db <- function(db_connection) {
  futile.logger::flog.debug('Initialising the database')

  DBI::dbGetQuery(db_connection, '
    CREATE TABLE IF NOT EXISTS bom_site(
      number INTEGER,  -- The site number
      name TEXT,       -- The full name of the site
      latitude REAL,   -- The latitude in degrees
      longitude REAL,  -- The longitude in degrees
      elevation REAL,  -- Elevation in metres
      p_c INTEGER      -- The last p_c value returned by the BOM, used to
                       -- download data
    );
  ')
  DBI::dbGetQuery(db_connection, '
    CREATE UNIQUE INDEX IF NOT EXISTS
      bom_site_unique_number
    ON
      bom_site(number);
  ')

  DBI::dbGetQuery(db_connection, '
    CREATE TABLE IF NOT EXISTS bom_rainfall(
      site_number INTEGER,    -- The site_number of the corresponding
                              -- bom_site row
      date TEXT,              -- A text date for this observation
      rainfall REAL,          -- The rainfall value for the day in mm
                              -- (may be blank)
      days_measured INTEGER,  -- The number of days the measured rainfall
                              -- corresponds to (NA for days with NA
                              -- or 0 rainfall)
      quality INTEGER,        -- Whether the value has been quality
                              -- checked by the BOM
      FOREIGN KEY(site_number) REFERENCES bom_site(number)
    );
  ')
  DBI::dbGetQuery(db_connection, '
    CREATE UNIQUE INDEX IF NOT EXISTS
      bom_rainfall_unique_site_date
    ON
      bom_rainfall(site_number, date);
  ')
}
