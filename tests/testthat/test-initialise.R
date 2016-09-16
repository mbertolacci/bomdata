context('initialise')

test_that('initialise', with_db(function(db_connection) {
    # Assumes that with_db initialised the data. List of tables should be correct
    expect_equal(
        DBI::dbListTables(db_connection),
        c('bom_rainfall', 'bom_site')
    )
}))
