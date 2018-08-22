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

run_add_climate_data_test <- function(add_site_fn, type) {
  with_db(function(db_connection) {
    expect_true(add_site_fn(db_connection))

    # Retrieve from the database
    data <- DBI::dbGetQuery(db_connection, sprintf('
      SELECT
        *
      FROM
        bom_%s
      WHERE
        site_number = 9519
    ', type))
    expect_equal(ncol(data), 5)
    expect_true(nrow(data) >= c(
      'rainfall' = 40777,
      'max_temperature' = 22511,
      'min_temperature' = 22511,
      'solar_exposure' = 10460
    )[type])
    expect_is(data[, type], 'numeric')
  })
}

for (type in c(
  'rainfall',
  'max_temperature',
  'min_temperature',
  'solar_exposure'
)) {
  test_that(
    sprintf('add_daily_climate_data for %s, site_number variant', type),
    run_add_climate_data_test(function(db_connection) {
      add_daily_climate_data(db_connection, 9519, type = type)
    }, type)
  )

  test_that(
    sprintf('add_daily_climate_data for %s, data variant', type),
    run_add_climate_data_test(function(db_connection) {
      data <- download_daily_climate_data(9519, type = type)
      add_daily_climate_data(
        db_connection,
        9519,
        data = data,
        type = type
      )
    }, type)
  )
}
