# HACK(mgnb): to allow tests to pass when using Rscript
library(methods)

context('util')

futile.logger::flog.threshold(futile.logger::ERROR, name = 'bomdata')

## load_many_sites

test_that('load_many_sites returns data', with_db(function(db_connection) {
  load_many_sites(
    db_connection,
    c(3003, 40780, 200340),
    n_parallel = 1
  )

  site_data <- get_site(db_connection, 3003)
  expect_equal(site_data[1, 'number'], 3003)
}))

## load_high_quality_rainfall

test_that(
  'load_high_quality_rainfall loads data',
  with_db(function(db_connection) {
    # NOTE(mgnb): this test is slow and that's annoying
    load_high_quality_rainfall(db_connection)

    site_count <- DBI::dbGetQuery(db_connection, '
      SELECT COUNT(*) FROM bom_site
    ')
    expect_true(site_count[1, 1] > 100)
  })
)
