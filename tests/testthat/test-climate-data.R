context('climate-data')

futile.logger::flog.threshold(
  futile.logger::ERROR,
  name = 'bomdata.climate-data'
)

## download_daily_climate_data

test_that('download_daily_climate_data gets rainfall data', {
  # NOTE(mgnb): this is Broome Airport (3003). Just check that it returns
  # something.
  data <- download_daily_climate_data(3003, type = 'rainfall')
  expect_equal(ncol(data), 4)
  # True as of 2016/08/08
  expect_true(nrow(data) >= 28345)
  expect_is(data[, 'rainfall'], 'numeric')
})

test_that('download_daily_climate_data gets max temperature data', {
  data <- download_daily_climate_data(9519, type = 'max_temperature')
  expect_equal(ncol(data), 4)
  # True as of 2018/08/20
  expect_true(nrow(data) >= 22511)
  expect_is(data[, 'max_temperature'], 'numeric')
})

test_that(
  'download_daily_climate_data returns zero rows when there is no data',
  {
    # NOTE(mgnb): this site, HYGAIT TM (40780), appears to have no data
    expect_equal(dim(download_daily_climate_data(40780)), c(0, 4))
  }
)

## add_daily_climate_data

run_rainfall_test <- function(load_site_fn) {
  with_db(function(db_connection) {
    # Load rainfall into the database
    expect_true(load_site_fn(db_connection))

    # Retrieve from the database
    data <- DBI::dbGetQuery(db_connection, '
      SELECT
        *
      FROM
        bom_rainfall
      WHERE
        site_number = 3003
    ')
    expect_equal(ncol(data), 5)
    expect_true(nrow(data) >= 28345)
    expect_is(data[, 'rainfall'], 'numeric')
  })
}

test_that(
  'add_daily_climate_data for rainfall, site_number variant',
  run_rainfall_test(function(db_connection) {
    add_daily_climate_data(db_connection, 3003, type = 'rainfall')
  })
)

test_that(
  'add_daily_climate_data for rainfall, data variant',
  run_rainfall_test(function(db_connection) {
    data <- download_daily_climate_data(3003, type = 'rainfall')
    add_daily_climate_data(
      db_connection,
      3003,
      data = data,
      type = 'rainfall'
    )
  })
)

run_max_temperature_test <- function(load_site_fn) {
  with_db(function(db_connection) {
    # Load max_temperature into the database
    expect_true(load_site_fn(db_connection))

    # Retrieve from the database
    data <- DBI::dbGetQuery(db_connection, '
      SELECT
        *
      FROM
        bom_max_temperature
      WHERE
        site_number = 9519
    ')
    expect_equal(ncol(data), 5)
    expect_true(nrow(data) >= 22511)
    expect_is(data[, 'max_temperature'], 'numeric')
  })
}

test_that(
  'add_daily_climate_data for max_temperature, site_number variant',
  run_max_temperature_test(function(db_connection) {
    add_daily_climate_data(db_connection, 9519, type = 'max_temperature')
  })
)

test_that(
  'add_daily_climate_data for max_temperature, data variant',
  run_max_temperature_test(function(db_connection) {
    data <- download_daily_climate_data(9519, type = 'max_temperature')
    add_daily_climate_data(
      db_connection,
      9519,
      data = data,
      type = 'max_temperature'
    )
  })
)
