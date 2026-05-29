# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# CARTOGRAPHY MODULE (REFACTORED) ----
# Purpose:
#   - Provide a cached, reusable cartography bundle for reporting
#   - Build and cache objects only when source data or code changes
#   - Expose a single public function: load_cartography()
#
# Cached Outputs (RDS):
#   - county_two.rds
#   - county_seven.rds
#   - zcta_fips.rds
#   - north_zip_codes.rds
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# 1. lintr options ----
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

# Disable lintr warnings for object usage in this module, as objects are built
# and used within the same function scope.

# nolint start: object_usage_linter

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# 2. Paths and cache files ----
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

lut_dir <- "P:/DATA/LUTs"

# Determine if the VPN is connected
if (
  !dir.exists(
    lut_dir
  )
) {
  stop(
    "LUT directory not found: ", lut_dir,
    "\nIs your VPN connected and the P: drive mounted?"
  )
}

# Determine ETL repo root depending on execution context
if (
  exists(
    "etl_dir"
  )
) {
  root <- etl_dir
} else {
  root <- here::here()
}

cache_dir <- file.path(
  root,
  "data_intermediate",
  "cartography"
)

if (
  !dir.exists(
    cache_dir
  )
) {
  dir.create(
    cache_dir,
    recursive = TRUE
  )
}

cache_files <- list(
  county_two = file.path(
    cache_dir,
    "county_two.rds"
  ),
  county_seven = file.path(
    cache_dir,
    "county_seven.rds"
  ),
  zcta_fips = file.path(
    cache_dir,
    "zcta_fips.rds"
  ),
  north_zip_codes = file.path(
    cache_dir,
    "north_zip_codes.rds"
  )
)

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# 3. Cache validation helpers ----
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

# Returns the timestamp of this module's source file.
cartography_source_timestamp <- function() {
  
  # Determine ETL repo root depending on execution context
  if (
    exists(
      "etl_dir"
      )
    ) {
    root <- etl_dir
  } else {
    root <- here::here()
  }
  
  src_path <- file.path(
    root,
    "R",
    "cartography.R"
    )
  
  if (
    !file.exists(
      src_path
      )
    ) {
    return(
      NA_real_
      )
  }
  
  file.info(
    src_path
    )$mtime
}

# Returns the timestamp of the ZIP metadata LUT (only remaining external dep).
cartography_lut_timestamp <- function() {
  zip_lut <- file.path(
    lut_dir,
    "MO ZIP Codes by County_City LUT.xlsx"
  )
  
  if (
    !file.exists(
      zip_lut
      )
    ) {
    return(
      NA_real_
      )
  }
  
  file.info(
    zip_lut
    )$mtime
}

# Returns the timestamp of the authoritative cached RDS file.
# We use zcta_fips.rds as the anchor because it depends on all upstream pieces.
cartography_cache_timestamp <- function() {
  anchor_path <- cache_files$zcta_fips
  
  if (
    !file.exists(
      anchor_path
      )
    ) {
    return(
      NA_real_
      )
  }
  
  file.info(
    anchor_path
    )$mtime
}

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Determines whether cached cartography objects can be reused.
# Cache is valid when:
#   1. All expected RDS files exist
#   2. The anchor cache file (zcta_fips.rds) is newer than:
#        - The ZIP Code metadata LUT
#        - The cartography.R source file
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
cartography_cache_is_valid <- function() {
  
  # 1. All cache files must exist
  if (
    !all(
      file.exists(
        unlist(
          cache_files
          )
        )
      )
    ) {
    return(
      FALSE
      )
  }
  
  cache_ts <- cartography_cache_timestamp()
  lut_ts   <- cartography_lut_timestamp()
  src_ts   <- cartography_source_timestamp()
  
  # 2. All timestamps must be available
  if (
    is.na(
      cache_ts
      ) ||
    is.na(
      lut_ts
      ) ||
    is.na(
      src_ts
      )
    ) {
    return(
      FALSE
      )
  }
  
  # 3. Cache must be at least as new as both LUT and source
  cache_ts >= lut_ts && cache_ts >= src_ts
}

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# 4. Core ETL builders ----
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

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
        ), # AFFGEOID20 or GEOIDFQ20
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

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# 5. Build full cartography bundle ----
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Function build cartography:
#
# Performs the full ETL pipeline:
#   - Reads curated ZIP metadata
#   - Builds county shapefiles
#   - Builds ZCTA shapefiles
#   - Constructs ZIP → County → ZCTA crosswalk
#   - Adds Promise Zone and North County flags
#   - Writes all outputs to cached RDS files
#
# Called automatically when:
#   - Cache is missing
#   - ZIP metadata LUT changes
#   - cartography.R changes
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

build_cartography <- function() {
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

  saveRDS(
    county_two,
    cache_files$county_two
  )
  saveRDS(
    county_seven,
    cache_files$county_seven
  )
  saveRDS(
    zcta_fips,
    cache_files$zcta_fips
  )
  saveRDS(
    north_zip_codes,
    cache_files$north_zip_codes
  )

  invisible(
    list(
      county_two = county_two,
      county_seven = county_seven,
      zcta_fips = zcta_fips,
      north_zip_codes = north_zip_codes
    )
  )
}

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# 6. Public loader
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

load_cartography <- function() {
  if (
    !cartography_cache_is_valid()
  ) {
    message(
      "Rebuilding cartography cache..."
    )
    return(
      build_cartography()
    )
  }

  message(
    "Loading cached cartography..."
  )

  list(
    county_two = readRDS(
      cache_files$county_two
      ),
    county_seven = readRDS(
      cache_files$county_seven
      ),
    zcta_fips = readRDS(
      cache_files$zcta_fips
      ),
    north_zip_codes = readRDS(
      cache_files$north_zip_codes
      )
  )
}

# End disabling of lintr warnings for object usage in this module.
# nolint end

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# END OF CARTOGRAPHY MODULE
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
