# ===
# CARTOGRAPHY MODULE (BUILDER FUNCTIONS ONLY) ----
# Purpose:
#   - Provide reusable cartography builder functions for reporting and ETL
#   - No caching, timestamp, or .rds logic (handled by {targets} in _targets.R)
#   - Expose pure functions that can be composed in pipelines
#
# Responsibilities:
#   - Read LUTs from P:/DATA/LUTs
#   - Build county shapefiles (2-county and 7-county subsets)
#   - Build ZCTA shapefiles
#   - Build ZIP → County → ZCTA crosswalk with Promise Zone and North flags
# ===

# ===
# 1. lintr options ----
# ===

# Disable lintr warnings for object usage in this module, as objects are built
# and used within the same function scope.

# nolint start: object_usage_linter

# ===
# 2. Paths ----
# ===

# Shared LUT directory on the enterprise server. Access to this path assumes
# that the P: drive is mounted (typically via VPN when remote).
lut_dir <- "P:/DATA/LUTs"

# ===
# 3. Core ETL builders ----
# ===

# Hard-coded Promise Zone ZIP Codes (stable, federally defined)
build_promise_zone_zips <- function() {
  tibble::tibble(
    pz_zip = c(
      "63042",
      "63044",
      "63101",
      "63102",
      "63103",
      "63106",
      "63107",
      "63108",
      "63112",
      "63113",
      "63114",
      "63115",
      "63120",
      "63121",
      "63130",
      "63133",
      "63134",
      "63135",
      "63136",
      "63137",
      "63138",
      "63140",
      "63145",
      "63147"
    ),
    promise_zone = 1
  )
}

# County FIPS LUT (still sourced from Excel)
build_mo_county_fips_lut <- function() {
  fips_path <- file.path(
    lut_dir,
    "MO County FIPS LUT.xlsx"
  )

  readxl::read_excel(
    fips_path,
    sheet = "fips_lut",
    na = c(
      "",
      " "
    )
  ) |>
    dplyr::mutate(
      area_name = dplyr::if_else(
        county_fips_code == "510",
        "St. Louis City",
        area_name
      )
    ) |>
    dplyr::filter(
      area_type == "County" | area_name == "St. Louis City"
    ) |>
    dplyr::select(
      -summary_level,
      -county_subdivision_fips_code,
      -place_fips_code,
      -area_type
    ) |>
    dplyr::rename(
      county = area_name,
      county_fips = county_fips_code,
      state_fips = state_fips_code
    ) |>
    tidyr::unite(
      "geoid",
      state_fips:county_fips,
      sep = "",
      na.rm = TRUE,
      remove = FALSE
    )
}

# ZIP to County + City/Neighborhood metadata (manually curated list)
build_mo_county_zip_lut <- function() {
  zip_path <- file.path(
    lut_dir,
    "MO ZIP Codes by County_City LUT.xlsx"
  )

  readxl::read_excel(
    zip_path,
    sheet = "MO ZIP Codes",
    na = c(
      "",
      " "
    )
  ) |>
    dplyr::distinct(
      zip_code,
      .keep_all = TRUE
    ) |>
    dplyr::mutate(
      zip_code = as.character(
        zip_code
      )
    )
}

# Hard-coded North County ZIPs based on local knowledge and review of
# ZIP to County crosswalks.
build_north_zip_codes <- function() {
  tibble::tibble(
    zip_code = c(
      "63031",
      "63033",
      "63034",
      "63042",
      "63044",
      "63074",
      "63114",
      "63121",
      "63132",
      "63133",
      "63134",
      "63135",
      "63136",
      "63137",
      "63138",
      "63140",
      "63108",
      "63103",
      "63113",
      "63155",
      "63110",
      "63115",
      "63106",
      "63107",
      "63120",
      "63117",
      "63112",
      "63101",
      "63147",
      "63102"
    ),
    north_zip_code = 1
  )
}

# County shapefiles (minimal fields, geometry retained for mapping)
# Preserves the logic for county_two and county_seven subsets.
build_county_shapefiles <- function() {
  county_raw <- tigris::counties(
    state = "29",
    cb = TRUE,
    year = NULL
  ) |>
    dplyr::select(
      state_fips = STATEFP,
      county_fips = COUNTYFP,
      geoid = GEOID,
      county_name = NAME,
      geometry
    ) |>
    dplyr::mutate(
      county_name = dplyr::if_else(
        county_fips == "510" &
          county_name == "St. Louis",
        "St. Louis City",
        county_name
      )
    )

  county_two <- county_raw |>
    dplyr::filter(
      county_fips %in% c(
        "189",
        "510"
      )
    )

  county_seven <- county_raw |>
    dplyr::filter(
      county_fips %in% c(
        "071",
        "099",
        "113",
        "183",
        "189",
        "219",
        "510"
      )
    )

  list(
    county_two = county_two,
    county_seven = county_seven
  )
}

# ZCTA shapefiles (`year = NULL` so that it will download whatever vintage is
# default). The rename() of `geoid` has been updated so that this function works
# regardless of whether `cb = TRUE` or `cb = FALSE`.
build_zcta <- function() {
  tigris::zctas(
    cb = FALSE,
    starts_with = c(
      "63",
      "64",
      "65"
    ),
    year = NULL
  ) |>
    dplyr::rename(
      zcta = dplyr::starts_with(
        "ZCTA"
      ),
      geoid = matches(
        "^GEOID[0-9]{2}$"
      ),      # GEOID20
      geoid_aff = matches(
        "AFFGEOID|GEOIDFQ"
      ),      # AFFGEOID20 or GEOIDFQ20
      land_area = dplyr::starts_with(
        "ALAND"
      ),
      water_area = dplyr::starts_with(
        "AWATER"
      )
    ) |>
    dplyr::select(
      -geoid
    )
}

# Build ZCTA → County/Zone crosswalk
build_zcta_fips <- function(
  zcta,
  mo_county_fips_zip_crosswalk,
  pz_zips,
  north_zip_codes
) {
  zcta |>
    dplyr::left_join(
      mo_county_fips_zip_crosswalk,
      by = c(
        "zcta" = "zip_code"
      )
    ) |>
    dplyr::left_join(
      pz_zips,
      by = c(
        "zcta" = "pz_zip"
      )
    ) |>
    dplyr::mutate(
      promise_zone = tidyr::replace_na(
        promise_zone,
        0
      )
    ) |>
    dplyr::left_join(
      north_zip_codes,
      by = c(
        "zcta" = "zip_code"
      )
    ) |>
    dplyr::mutate(
      north_zip_code = tidyr::replace_na(
        north_zip_code,
        0
      )
    )
}

# ===
# 4. High-level cartography builder (optional convenience) ----
# ===

# This function does NOT cache or write to disk. It simply orchestrates the
# builders and returns a named list. {targets} will decide when/how to run it.
build_cartography_bundle <- function() {
  pz_zips <- build_promise_zone_zips()
  mo_county_fips_lut <- build_mo_county_fips_lut()
  mo_county_zip_lut <- build_mo_county_zip_lut()
  north_zip_codes <- build_north_zip_codes()

  mo_county_fips_zip_crosswalk <- dplyr::left_join(
    mo_county_zip_lut,
    mo_county_fips_lut,
    by = "county"
  )

  county_shapes <- build_county_shapefiles()
  county_two <- county_shapes$county_two
  county_seven <- county_shapes$county_seven

  zcta <- build_zcta()

  zcta_fips <- build_zcta_fips(
    zcta,
    mo_county_fips_zip_crosswalk,
    pz_zips,
    north_zip_codes
  )

  list(
    county_two = county_two,
    county_seven = county_seven,
    zcta_fips = zcta_fips,
    north_zip_codes = north_zip_codes
  )
}

# End disabling of lintr warnings for object usage in this module.
# nolint end

# ===
# END OF CARTOGRAPHY MODULE
# ===
