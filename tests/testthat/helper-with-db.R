with_db <- function(fn) {
  db_connection <- DBI::dbConnect(RSQLite::SQLite(), tempfile())
  initialise_db(db_connection)
  invisible(fn(db_connection))
  teardown(invisible(DBI::dbDisconnect(db_connection)))
}
