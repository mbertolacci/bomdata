# This file contains look-up tables for internal BOM codes

NCC_OBS_CODES <- c(
  'rainfall' = 136,
  'max_temperature' = 122,
  'min_temperature' = 123,
  'solar_exposure' = 193
)

.get_ncc_obs_code <- function(
  type = c('rainfall', 'max_temperature', 'min_temperature', 'solar_exposure')
) {
  type <- match.arg(type)
  NCC_OBS_CODES[type]
}

PRODUCT_CODES <- c(
  'rainfall' = 'IDCJAC0009',
  'max_temperature' = 'IDCJAC0010',
  'min_temperature' = 'IDCJAC0011',
  'solar_exposure' = 'IDCJAC0016'
)

.get_product_code <- function(
  type = c('rainfall', 'max_temperature', 'min_temperature', 'solar_exposure')
) {
  type <- match.arg(type)
  PRODUCT_CODES[type]
}
