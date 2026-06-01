# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# CCSR MODULE (BUILDER FUNCTIONS ONLY) ----
# Purpose:
#   - Provide a reusable builder for the CCSR diagnosis lookup table
#   - No caching, timestamp, or .rds logic (handled by {targets} in _targets.R)
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

# nolint start: object_usage_linter

# Shared LUT directory on the enterprise server
ccsr_lut_dir <- "P:/DATA/LUTs/Latest DXCCSR LUT"

build_ccsr_dx_lut <- function() {

  ccsr_dx_lut_file_list <- list.files(
    path = ccsr_lut_dir,
    pattern = "\\.CSV$",
    full.names = TRUE
  )

  if (length(ccsr_dx_lut_file_list) == 0) {
    stop(
      "No CCSR DX LUT CSV files found in: ",
      ccsr_lut_dir
    )
  }

  ccsr_dx_lut_file_list |>
    purrr::map(
      readr::read_csv,
      show_col_types = FALSE
    ) |>
    dplyr::bind_rows(
      .id = "id"
    ) |>
    dplyr::select(
      -id
    ) |>
    dplyr::rename_with(
      ~ gsub(
        "-",
        " ",
        .x
      )
    ) |>
    dplyr::rename_with(
      ~ gsub(
        "[[:punct:]]",
        "",
        .x
      )
    ) |>
    dplyr::rename_with(
      ~ tolower(
        gsub(
          " ",
          "_",
          .x,
          fixed = TRUE
        )
      )
    ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::everything(),
        ~ stringr::str_replace_all(
          .,
          "'",
          ""
        )
      )
    )
}

# nolint end

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# END OF CCSR MODULE
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
