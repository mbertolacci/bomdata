#' Initialise a bomdata database.
#'
#' Initialises a bomdata database. The database can have been initialised in
#' the past, so it's safe to call this again.
#' @param db_connection An RSQLite connection.
#' @export
initialise_db <- function(db_connection) {
  flog.debug('Initialising the database')

  DBI::dbExecute(db_connection, '
    CREATE TABLE IF NOT EXISTS bom_site(
      number INTEGER,  -- The site number
      name TEXT,       -- The full name of the site
      latitude REAL,   -- The latitude in degrees
      longitude REAL,  -- The longitude in degrees
      elevation REAL   -- Elevation in metres
    );
  ')
  DBI::dbExecute(db_connection, '
    CREATE UNIQUE INDEX IF NOT EXISTS
      bom_site_unique_number
    ON
      bom_site(number);
  ')

  DBI::dbExecute(db_connection, '
    CREATE TABLE IF NOT EXISTS bom_site_p_c(
      site_number INTEGER,  -- The site number
      type TEXT,            -- The observation type (rainfall, etc)
      p_c INTEGER,          -- The value of p_c
      FOREIGN KEY(site_number) REFERENCES bom_site(number)
    );
  ')
  DBI::dbExecute(db_connection, '
    CREATE UNIQUE INDEX IF NOT EXISTS
      bom_site_p_c_unique_number_code
    ON
      bom_site_p_c(site_number, type);
  ')

  for (type in c('rainfall', 'max_temperature')) {
    DBI::dbExecute(db_connection, sprintf('
      CREATE TABLE IF NOT EXISTS bom_%1$s(
        site_number INTEGER,    -- The site_number of the corresponding
                                -- bom_site row
        date TEXT,              -- A text date for this observation
        %1$s REAL,              -- The value for the day
                                -- (may be blank)
        days_measured INTEGER,  -- The number of days the value corresponds to
                                -- (NA for days with NA or 0 rainfall)
        quality INTEGER,        -- Whether the value has been quality
                                -- checked by the BOM
        FOREIGN KEY(site_number) REFERENCES bom_site(number)
      );
    ', type))
    DBI::dbExecute(db_connection, sprintf('
      CREATE UNIQUE INDEX IF NOT EXISTS
        bom_%1$s_unique_site_date
      ON
        bom_%1$s(site_number, date);
    ', type))
  }

  invisible(NULL)
}
