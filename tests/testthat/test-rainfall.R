context('rainfall')

futile.logger::flog.threshold(futile.logger::ERROR, name = 'bomdata.rainfall')

## bomdata_get_site_raw

test_that('bomdata_get_rainfall_raw gets data', {
    # NOTE(mgnb): this is Broome Airport (3003). Just check that it returns something.
    rainfall_data <- bomdata_get_rainfall_raw(bomdata_get_site_raw(3003))

    expect_equal(ncol(rainfall_data), 4)
    # True as of 2016/08/08 (I suppose it could go down if they withdraw data, but meh)
    expect_true(nrow(rainfall_data) >= 28345)
    expect_is(rainfall_data[, 'rainfall'], 'numeric')
})

test_that('bomdata_get_rainfall_raw returns NULL when there is no data', {
    # NOTE(mgnb): this site, HYGAIT TM (40780), appears to have no data
    expect_null(bomdata_get_rainfall_raw(bomdata_get_site_raw(40780)))
})

## bomdata_load_rainfall

run_site_test <- function(load_site_fn) {
    with_db(function(db_connection) {
        # Load site
        bomdata_load_site(db_connection, 3003)
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

test_that('bomdata_load_rainfall, site_number variant', run_site_test(function(db_connection) {
    bomdata_load_rainfall(db_connection, site_number = 3003)
}))

test_that('bomdata_load_rainfall, site_data variant', run_site_test(function(db_connection) {
    bomdata_load_rainfall(db_connection, site_data = bomdata_get_site_raw(3003))
}))

test_that('bomdata_load_rainfall, rainfall_data variant', run_site_test(function(db_connection) {
    rainfall_data <- bomdata_get_rainfall_raw(bomdata_get_site_raw(3003))
    bomdata_load_rainfall(
        db_connection,
        site_data = bomdata_get_site_raw(3003),
        rainfall_data = rainfall_data
    )
}))
