context('initialise')

test_that('initialise', with_db(function(db_connection) {
  # Assumes that with_db initialised the data. List of tables should be correct
  expect_equal(
    DBI::dbListTables(db_connection),
    c(
      'bom_max_temperature',
      'bom_min_temperature',
      'bom_rainfall',
      'bom_site',
      'bom_site_p_c',
      'bom_solar_exposure'
    )
  )
}))
