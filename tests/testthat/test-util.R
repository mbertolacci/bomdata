context('util')

futile.logger::flog.threshold(futile.logger::ERROR, name = 'bomdata')

## bomdata_load_many_sites

test_that('bomdata_load_many_sites returns data', with_db(function(db_connection) {
    bomdata_load_many_sites(
        db_connection,
        c(3003, 40780, 200340),
        n_parallel = 1
    )

    site_data <- bomdata_get_site(db_connection, 3003)
    expect_equal(site_data[1, 'number'], 3003)
}))
