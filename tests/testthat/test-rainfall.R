# HACK(mgnb): to allow tests to pass when using Rscript
library(methods)

context('rainfall')

futile.logger::flog.threshold(futile.logger::ERROR, name = 'bomdata.rainfall')

## download_site

test_that('download_rainfall gets data', {
  # NOTE(mgnb): this is Broome Airport (3003). Just check that it returns
  # something.
  rainfall_data <- download_rainfall(download_site(3003))

  expect_equal(ncol(rainfall_data), 4)
  # True as of 2016/08/08 (I suppose it could go down if they withdraw data,
  # but meh)
  expect_true(nrow(rainfall_data) >= 28345)
  expect_is(rainfall_data[, 'rainfall'], 'numeric')
})

test_that('download_rainfall returns NULL when there is no data', {
  # NOTE(mgnb): this site, HYGAIT TM (40780), appears to have no data
  expect_null(download_rainfall(download_site(40780)))
})

## load_rainfall

run_site_test <- function(load_site_fn) {
  with_db(function(db_connection) {
    # Load site
    add_site(db_connection, 3003)
    # Load rainfall into the database
    expect_true(load_site_fn(db_connection))

    # Retrieve from the database
    rainfall_data <- DBI::dbGetQuery(db_connection, '
      SELECT
        *
      FROM
        bom_rainfall
      WHERE
        site_number = 3003
    ')
    expect_equal(ncol(rainfall_data), 5)
    expect_true(nrow(rainfall_data) >= 28345)
    expect_is(rainfall_data[, 'rainfall'], 'numeric')
  })
}

test_that(
  'load_rainfall, site_number variant',
  run_site_test(function(db_connection) {
    add_rainfall(db_connection, site_number = 3003)
  })
)

test_that(
  'load_rainfall, site_data variant',
  run_site_test(function(db_connection) {
    add_rainfall(db_connection, site_data = download_site(3003))
  })
)

test_that(
  'load_rainfall, rainfall_data variant',
  run_site_test(function(db_connection) {
    rainfall_data <- download_rainfall(download_site(3003))
    add_rainfall(
      db_connection,
      site_data = download_site(3003),
      rainfall_data = rainfall_data
    )
  })
)
