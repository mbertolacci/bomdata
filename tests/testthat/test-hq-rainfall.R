context('hq-rainfall')

futile.logger::flog.threshold(futile.logger::ERROR, name = 'bomdata')

## add_high_quality_rainfall

test_that(
  'add_high_quality_rainfall loads data',
  with_db(function(db_connection) {
    skip('Test takes a very long time')

    # NOTE(mgnb): this test is slow and that's annoying
    add_high_quality_rainfall(db_connection)

    site_count <- DBI::dbGetQuery(db_connection, '
      SELECT COUNT(*) FROM bom_site
    ')
    expect_true(site_count[1, 1] > 100)
  })
)
