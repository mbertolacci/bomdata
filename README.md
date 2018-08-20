# bomdata

[![Build Status](https://travis-ci.org/mbertolacci/bomdata.svg?branch=master)](https://travis-ci.org/mbertolacci/bomdata)
[![codecov](https://codecov.io/gh/mbertolacci/bomdata/branch/master/graph/badge.svg)](https://codecov.io/gh/mbertolacci/bomdata)

## Installation

This package is not in CRAN right now. Just install using devtools and github

    devtools::install_github('mbertolacci/bomdata')

## Usage

This package helps you build a SQLite database of site metadata and rainfall data. It doesn't help you with SQL itself, though there are a few examples in this README to that end.

### Basic usage

To initialise a new database, run

    db_connection <- DBI::dbConnect(RSQLite::SQLite(), 'bomdata.db')
    bomdata::initialise_db(db_connection)

This will give you a blank database, ready to be loaded.

You can load the metadata into the database for a single site like so

    bomdata::add_site(db_connection, site_number = 3003)

and then query it like so

    site_data <- DBI::dbGetQuery(db_connection, '
        SELECT
            *
        FROM
            bom_site
    ')
    print(head(site_data))

You can then add rainfall data by running

    bomdata::add_daily_climate_data(db_connection, 3003, type = 'rainfall')

and now you will find its rainfall data in the database

    data <- DBI::dbGetQuery(db_connection, '
        SELECT
            *
        FROM
            bom_rainfall
        WHERE
            site_number = 3003
    ')
    print(tail(data))

### Bulk downloads

You can load the metadata and rainfall data for many sites at once by running

    bomdata::add_many_sites(db_connection, c(3003, 9031))
    site_data <- DBI::dbGetQuery(db_connection, '
        SELECT
            *
        FROM
            bom_site
    ')
    print(head(site_data))

A useful facility for listing sites is in the form of listing site numbers in a region, which can be done by running

    site_numbers <- bomdata::get_site_numbers_in_region('NSW')
    print(head(site_numbers))

The recognised regions are 'AUS', 'WA', 'SA', 'VIC', 'NSW', 'NT', 'QLD', 'TAS', and 'ANT'.
