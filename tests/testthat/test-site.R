context('site')

futile.logger::flog.threshold(futile.logger::ERROR, name = 'bomdata.site')

## bomdata_get_site_raw

test_that('bomdata_get_site_raw returns data', {
    # NOTE(mgnb): this is Broome Airport (3003). Just check that it returns something.
    site_data <- bomdata_get_site_raw(3003)

    # Should get the right site
    expect_equal(site_data[1, 'number'], 3003)

    # Should successfully extract the p_c number
    expect_false(is.na(site_data[1, 'p_c']))
})

test_that('bomdata_get_site_raw returns nothing for sites with no data', {
    # NOTE(mgnb): this is Rabaul (200340).
    site_data <- bomdata_get_site_raw(200340)
    expect_null(site_data)
})

test_that('bomdata_get_site_raw returns nothing for sites that are not in the return table', {
    # NOTE(mgnb): this is Coldsteam Comparison (86320). I'm not sure what the deal is with these sites. They are very
    # close to other existing sites (in this case Coldstream (86383)), but have no data.
    site_data <- bomdata_get_site_raw(86320)
    expect_null(site_data)
})

## bomdata_get_site_numbers_in_region

test_that('bomdata_get_site_numbers_in_region', {
    # Just do one sample reason and make sure we get back an integer vector
    site_numbers <- bomdata_get_site_numbers_in_region('TAS')
    expect_true(length(site_numbers) > 0)
    expect_is(site_numbers, 'integer')
})

## bomdata_load_site

run_load_site_test <- function(load_site_fn) {
    with_db(function(db_connection) {
        # Load into the database
        expect_true(load_site_fn(db_connection))
        # Retrieve from the database
        site_data <- bomdata_get_site(db_connection, 3003)
        expect_equal(site_data[1, 'number'], 3003)
    })
}

test_that('bomdata_load_site, site_number variant', run_load_site_test(function(db_connection) {
    bomdata_load_site(db_connection, site_number = 3003)
}))

test_that('bomdata_load_site, site_data variant', run_load_site_test(function(db_connection) {
    bomdata_load_site(db_connection, site_data = bomdata_get_site_raw(3003))
}))

## bomdata_get_site
test_that('bomdata_get_site', with_db(function(db_connection) {
    bomdata_load_site(db_connection, 3003)
    site_data <- bomdata_get_site(db_connection, 3003)
    expect_equal(site_data[1, 'number'], 3003)
}))