# ===
# COMPLEX CARE ETL PIPELINE (Refactored, Metadata-Driven) ----
#
# This script implements a metadata-driven column typing ETL for COMPLEX CARE,
# returning a nested list of raw and transformed objects instead of writing to
# the global environment as has been done in the past. It replaces the legacy
# monolithic ETL child doc with a clean, testable, and maintainable workflow.
#
# ===
#
#  Core design principles:
#   - PathClient is the authoritative event timeline (one row per enrollment)
#   - TIEDENROLLMENT is the canonical episode join key
#   - Form tables (roster, complex_care_clinical_notes,
#       complex_care_mercy_beacn_benchmarks, etc.) are joined to PathClient by
#       TIEDENROLLMENT
#   - SCD summation tables (payor, housing) are joined by
#       PARENT_DOCSERNO to all possible parent forms
#       (complex_care_mercy_beacn_benchmarks)
#   - Active SCD summation tables are joined into complex_care_full_data (one
#       row per enrollment); “all” SCD summation tables remain long form for
#       reporting
#   - All form columns are prefixed with form name (roster_, ccnotes_,
#       benchmarks_, payor_, housing_, etc.)
#
#
# ===
#
# HOW THIS SCRIPT IS ORGANIZED
#
# 1. File paths
#      - All COMPLEX CARE extract paths are defined in `complex_care_paths`,
#        using make_path()
#
# 2. Ingestion functions (load_complex_care_*)
#      - Each function loads one COMPLEX CARE extract using metadata from
#        analytic_fields (loaded via load_analytic_fields() in helpers.R)
#      - No column types are hard-coded; all typing is metadata-driven
#      - No renaming or cleanup should occur here
#
# 3. Transformation functions (transform_complex_care_*)
#      - Each function performs one major transformation step:
#          * transform_complex_care_pathclient()
#          * transform_complex_care_referral_flow()
#      - These functions join related extracts
#      - They return lists so Data Team staff can inspect intermediate objects
#      - complex-care-pathclient is pivoted to ensure one row per enrollment
#      - Event forms are cleaned to drop notes fields and *_code fields, prefix
#          all columns with form name, and preserve only client_number and
#          tiedenrollment as join keys
#      - SCD summation tables are cleaned to rename parent_docserno, prefix all
#          columns, and drop client_number from each. parent_docsernos are
#          joined to the first non-NA docserno from among the parent forms
#
# 4. Semantic wrapper function extract_complex_care_full_data() returns the
#      final, analysis-ready, wide COMPLEX CARE dataset (one row per
#      enrollment). Subsetting may be performed in the parent report projects
#      using the build_subsets() function in helpers.R.
#
# 5. Entry point
#      - run_complex_care_etl(...)
#      - Orchestrates ingestion → transformation → assembly
#      - Designed to be used as a {targets} target (e.g., complex_care_etl)
#      - Returns a nested list of all COMPLEX CARE objects; writing .rds files
#          is handled elsewhere in the ETL repo
#
# ===
#
# INSPECTING INTERMEDIATE OBJECTS
#
# The ETL returns a nested list so Data Team staff can inspect intermediate
#   objects without relying on global environment side effects.
#
# Example:
#   * complex_care <- run_complex_care_etl(complex_care_paths)
#
# Inspect raw ingestion tibbles:
#   * View(complex_care$raw$complex_care_client) # raw complex_care_client
#   * View(complex_care$raw$complex_care_pathclient) # raw
#       complex_care_pathclient
#
# Inspect intermediate transformations:
#   * View(complex_care$transform$pathclient$joined_pathclient) # pivoted
#     complex_care_pathclient
#   * View(complex_care$transform$referral_flow$joined_referral_flow)
#       # full joined pathclient with pathway event tibbles and scd tables
#
# Inspect final full dataset:
#   * View(complex_care$complex_care_full_data) # wide, one row per enrollment
#
# This structure is intended to make debugging, onboarding, and unit testing
#   straightforward and to avoid reliance on global environment.
#
# However, if needed, one may assign objects to the global environment:
#   * complex_care_pathclient_raw <- complex_care$raw$complex_care_pathclient
#
# ===
#
# ABOUT COMPLEX CARE DATA STRUCTURE
#
# COMPLEX CARE enrollments are composed of multiple data sources:
#
#   * providerPlacement (program enrollment and dismissal - not joined)
#   * client (demographics)
#   * pathClient (Pathway metadata bridge)
#   * roster
#   * complex_care_clinical_notes
#   * complex_care_mercy_beacn_benchmarks
#   * complex_care_pfp_discharge
#   * complex_care_quality_of_life
#   * complex_care_shelter_beds
#   * housing and payor Source (active/all)
#
# The transformation layer reconstructs this program life cycle for each
#   enrollment.
#
# ===
#
# REPORTING SUBSETS
#
# COMPLEX CARE reporting uses two primary fiscal-period subsets:
#
#   1. dismissed_within_period
#        - Used for outcomes
#        - Includes all enrollments dismissed in the fiscal period
#
#   2. initiated_within_period
#        - Used for referral flow and program management
#        - Includes all enrollments initiated in the fiscal period,
#          regardless of whether they remain active or were dismissed
#
# Additional subsets are also possible but have not been added as of this
#   writing.
#
# ===

# ===
# 1. List file paths for all data source files. ----
#   - Uses function make_path() from helpers.R.
# ===
complex_care_paths <- list(
  complex_care_provider_placement = make_path(
    "FAMCare Q_ProviderPlacement_BHN/",
    "Q_ProviderPlacement_BHN.csv"
  ),
  complex_care_pathclient = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_PATHCLIENT_ENROLLMENTS.csv"
  ),
  complex_care_pathway_docsernos = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_PATHWAY_FORM_DOCSERNOS.csv"
  ),
  complex_care_client = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_CLIENT.csv"
  ),
  complex_care_roster = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_ROSTER.csv"
  ),
  complex_care_clinical_notes = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_CLINICAL_NOTES.csv"
  ),
  complex_care_mercy_beacn_benchmarks = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_MERCY_BEACN_BENCHMARKS.csv"
  ),
  complex_care_pfp_discharge = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_PFP_DISCHARGE.csv"
  ),
  complex_care_quality_of_life = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_QUALITY_OF_LIFE.csv"
  ),
  complex_care_shelter_beds = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_SHELTER_BEDS.csv"
  ),
  complex_care_active_housing = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_ACTIVE_HOUSING_STATUS.csv"
  ),
  complex_care_all_housing = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_ALL_HOUSING_STATUS.csv"
  ),
  complex_care_active_payor_source = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_ACTIVE_PAYOR_SOURCE.csv"
  ),
  complex_care_all_payor_source = make_path(
    "FAMCare COMPLEX CARE Extract/",
    "Q_COMPLEX_CARE_ALL_PAYOR_SOURCE.csv"
  ),
  complex_care_ext_mercy_utilization = make_latest_file_path(
    "Clinical BEACN Extract/",
    pattern = "BH_UTILIZATION_ALL.*\\.(csv|xlsx)$"
  ),
  complex_care_ext_atd_notifications = make_all_file_paths(
    "EXT ATD Notifications Report",
    pattern = "ext_atd_notifications_\\d{8}\\.csv$"
  ),
  complex_care_ext_atd_watchlist_uploads = make_latest_file_path(
    "EXT ATD Watchlist Uploads",
    pattern = "ext_atd_watchlist_\\d{8}\\.csv$"
  ),
  complex_care_ext_pfp_service_history = make_all_file_paths(
    "EXT PFP Service History Report",
    pattern = "ext_pfp_service_history_report_\\d{8}\\.(csv|xlsx|xls)$"
  )
)

# ===
# 2. Ingestion/Loading Functions ----
# ===

# ===
# Ingest complex_care_client ----
#   - one row per client
# ===
load_complex_care_client <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_client,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_provider_placement ----
#   - one row per enrollment - available to supplement complex_care_pathclient 
#       but not joined
#   - Renames key fields
# ===
load_complex_care_provider_placement <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_provider_placement,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_pathclient ----
#   - Renames key fields
#   - Pivoting handled separately, so this is not one row per enrollment yet
# ===
load_complex_care_pathclient <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_pathclient,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_pathway_docsernos ----
#   - one row per Pathway Event form
# ===
load_complex_care_pathway_docsernos <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_pathway_docsernos,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_roster ----
#   - one row per roster for each enrollment
# ===
load_complex_care_roster <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_roster,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_clinical_notes ----
#   - one row per note for each enrollment
# ===
load_complex_care_clinical_notes <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_clinical_notes,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_mercy_beacn_benchmarks ----
#   - one row per metrics for each enrollment
# ===
load_complex_care_mercy_beacn_benchmarks <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_mercy_beacn_benchmarks,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_pfp_discharge ----
#   - one row per presenting concerns for each enrollment
# ===
load_complex_care_pfp_discharge <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_pfp_discharge,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_quality_of_life ----
#   - one row per event for each enrollment
# ===
load_complex_care_quality_of_life <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_quality_of_life,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_shelter_beds ----
#   - multiple rows per client counseling session for each enrollment
# ===
load_complex_care_shelter_beds <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_shelter_beds,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_active_payor_source ----
#   - one row per active payor source per enrollment
#   - Renames key fields
# ===
load_complex_care_active_payor_source <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_active_payor_source,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_all_payor_source ----
#   - long form with one row per payor source record per enrollment, which
#       means that this duplicates on enrollments
# ===
load_complex_care_all_payor_source <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_all_payor_source,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_active_housing_status ----
#   - one row per active housing status per enrollment
# ===
load_complex_care_active_housing <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_active_housing,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_all_housing_status ----
#   - long form with one row per housing status record per enrollment, which
#       means that this duplicates on enrollments
# ===
load_complex_care_all_housing <- function(
  complex_care_paths,
  analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_all_housing,
    analytic_fields = analytic_fields
  )
}

# ===
# Ingest complex_care_ext_mercy_utilization ----
#   - multiple rows per mrn
#   - Unlike other loaders, this one extracts the date from the file name
# ===
load_complex_care_ext_mercy_utilization <- function(
    complex_care_paths,
    analytic_fields
) {
  
  df <- load_famcare_extract(
    path = complex_care_paths$complex_care_ext_mercy_utilization,
    analytic_fields = analytic_fields
  )
  
  # Extract YYYYMMDD from filename
  date_created <- ymd(
    stringr::str_extract(
      complex_care_paths$complex_care_ext_mercy_utilization,
      "\\d{8}"
    )
  )
  
  df <- df |>
    mutate(
      date_created = date_created
      )
  
  df
}

# ===
# Ingest complex_care_ext_atd_notifications ----
#   - multiple rows per mrn
# ===
load_complex_care_ext_atd_notifications <- function(
    complex_care_paths,
    analytic_fields
) {
  purrr::map_dfr(
    complex_care_paths$complex_care_ext_atd_notifications,
    ~ load_famcare_extract(
      .x,
      analytic_fields
    )
  )
}

# ===
# Ingest complex_care_ext_atd_watchlist ----
#   - one rows per mrn
# ===
load_complex_care_ext_atd_watchlist <- function(
    complex_care_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_ext_atd_watchlist,
    analytic_fields = analytic_fields
  )
}

# ===
  # Ingest pfp_service_history_report ----
#   - multiple rows per mrn
# ===
load_complex_care_ext_pfp_service_history <- function(
    complex_care_paths,
    analytic_fields
) {
  load_famcare_extract(
    path = complex_care_paths$complex_care_ext_pfp_service_history,
    analytic_fields = analytic_fields
  )
}

# ===
# 3. Transformation Layer Overview ----
#
# The transformation layer converts raw Complex Care extracts (loaded via metadata-driven
# ingestion) into analysis-ready datasets.
#
# This layer is intentionally modular. Each function performs one major
# transformation step so that:
#   - Data Team staff can debug intermediate objects
#   - each step can be tested independently
#   - the ETL pipeline is readable and maintainable
#
# The major transformation steps are: 1. transform_complex_care_pathclient()
#        - joins client demographics and relocates to left side of tibble
#        - pivots Pathway Event form docsernos to columns
#        - retains only client demographics + enrollment metadata + docsernos
#
# 2. transform_complex_care_referral_flow()
#        - joins Pathway Event forms (roster, benchmarks, pfp_discharge)
#        - joins SCD tables (presenting concerns, active payor, active housing)
#
# 5. build_complex_care_full_data()
#        - returns joined_referral_flow as a unified dataset
#
# 6. build_complex_care_subsets() (optional)
#        - constructs reporting subsets, including:
#            * dismissed-within-fiscal-year (for outcomes)
#            * initiated-within-fiscal-year (for referral flow)
#
# All intermediate objects are returned as list elements so Data Team staff can
# inspect them interactively during development.
#
# All transformed tables are also returned as list elements to allow for
# troubleshooting.
# ===

# ===
# Transform complex_care_pathclient ----
#   - PathClient is the authoritative event timeline (enrollment, dismissal,
#       pathway events)
#   - Pivot to one row per enrollment
#   - Drop Pathway metadata columns
#   - Keep analytic fields (enrollment dates, dismissal, agency, etc.)
# ===
transform_complex_care_pathclient <- function(
  complex_care
) {
  # Load raw pathclient extract, which is duplicated by Pathway Event form rows
  df <- complex_care$complex_care_pathclient |>
    filter(
      !is.na(
        tiedenrollment
      )
    )

  # Normalize event names from human readable event labels into canonical
  # column names that will become the *_docserno key. If new event types are
  # ever added for the program, they must be added here.
  df <- df |>
    dplyr::mutate(
      event_key = dplyr::recode(
        pwy_event,
        "Complex Care Roster" = "roster_docserno",
        "Clinical BEACN Metrics" = "pfp_metrics_docserno",
        "PfP Discharge" = "pfp_discharge_docserno"
      )
    )

  # Extract enrollment-level columns, which describe the enrollment itself.
  # These columns do not vary by event type, so one distinct row per
  # (client_number, tiedenrollment) is retained.
  enrollment_cols <- c(
    "client_number",
    "tiedenrollment",
    "client_last",
    "client_first",
    "enrollment_starting_date",
    "enrollment_ending_date",
    "dismissal_reason_description",
    "age_at_enrollment",
    "agency_description",
    "enroll_path_join_source",
    "pwy_start_date",
    "pwy_end_date",
    "program_worker_employee_number",
    "program_worker_last",
    "program_worker_first"
  )

  enrollment <- df |>
    dplyr::select(
      tidyselect::all_of(
        enrollment_cols
      )
    ) |>
    dplyr::distinct(
      client_number,
      tiedenrollment,
      .keep_all = TRUE
    )

  # Merge client demographics
  enrollment <- enrollment |>
    dplyr::left_join(
      dplyr::select(
        complex_care$complex_care_client,
        client_number,
        birth_date,
        gender_description,
        race_description,
        ethnicity_description,
        mrn_mercy,
        mrn_bjc,
        mrn_ssm,
        ssn,
        ssn_last_four,
        eto_case_num,
        street,
        street2,
        city,
        state,
        zip_code,
        county_description
      ),
      by = "client_number"
    ) |>
    dplyr::rename(
      dob = birth_date
    )

  # Pivot only the pwy_forms_docserno column to produce one column per
  # event_docserno (roster_docserno, pfp_metrics_docserno, etc.). There should
  # only be one docserno per event per enrollment. If duplicates exist,
  # values_fn = first(na.omit(.x)) selects the first non-NA value. Exception
  # reports should detect duplicates, but this ensures that duplicates do not
  # stop the pipeline.
  events <- df |>
    dplyr::select(
      client_number,
      tiedenrollment,
      event_key,
      pwy_forms_docserno
    ) |>
    dplyr::distinct() |>
    tidyr::pivot_wider(
      names_from = event_key,
      values_from = pwy_forms_docserno,
      values_fn = ~ first(
        na.omit(
          .x
        )
      ),
      values_fill = NA
    )

  # Merge enrollment-level data with event_level docserno columns. The join
  # be one-to-one on (client_number, tiedenrollment).
  wide <- enrollment |>
    dplyr::left_join(
      events,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    )

  # Relocate client demographics columns to the left side of the tibble
  wide <- wide |>
    dplyr::relocate(
      client_number,
      client_last,
      client_first,
      client_last,
      client_first,
      dob,
      gender_description,
      race_description,
      ethnicity_description,
      mrn_mercy,
      mrn_bjc,
      mrn_ssm,
      ssn,
      ssn_last_four,
      eto_case_num,
      street,
      street2,
      city,
      state,
      zip_code,
      county_description,
      .before = everything()
    )

  # Return the final wide pathclient tibble. Output structure:
  # $joined_pathclient
  list(
    joined_pathclient = wide
  )
}

# ===
# Transform referral flow ----
#   - Joins REF, IC, RP
#   - Prefixes all columns except tiedenrollment
#   - Joins SCD summation tables (presconcerns, payor, housing) to ALL parent
#       forms
#   - Collapses SCD summation tables to one active row per enrollment
# ===
transform_complex_care_referral_flow <- function(
  complex_care
) {

  # Helper to prefix all columns except tiedenrollment and client_number and
  # also to drop metadata
  clean_form <- function(
    df,
    prefix
  ) {

    # Identify *_code columns
    code_cols <- names(df)[endsWith(names(df), "_code")]

    # Identify *_description columns
    desc_cols <- names(df)[endsWith(names(df), "_description")]

    # Determine which *_code columns have matching *_description columns
    code_with_desc <- code_cols[
      sub(
        "_code$",
        "_description",
        code_cols
      ) %in% desc_cols
    ]

    df |>
      # Drop narrative fields
      dplyr::select(
        -tidyselect::contains(
          "notes"
        ),
      ) |>
      # Drop only *_code columns that have matching *_description columns
      dplyr::select(
        -tidyselect::all_of(
          code_with_desc
        )
      ) |>
      # Prefix all remaining columns except join keys
      dplyr::rename_with(
        ~ paste0(
          prefix,
          .x
        ),
        -tidyselect::any_of(
          c(
            "tiedenrollment",
            "client_number"
          )
        )
      )
  }

  # Clean each active SCD summation tables
  payor <- clean_form(
    complex_care$complex_care_active_payor_source,
    "payor_"
  ) |>
    dplyr::rename(
      parent_docserno = payor_parent_docserno
    ) |>
    dplyr::select(
      -client_number
    )

  housing <- clean_form(
    complex_care$complex_care_active_housing,
    "housing_"
  ) |>
    dplyr::rename(
      parent_docserno = housing_parent_docserno
    ) |>
    dplyr::select(
      -client_number
    )

  # ccnotes <- clean_form(
  #   complex_care$complex_care_clinical_notes,
  #   "ccnotes_"
  # ) |>
  #   dplyr::rename(
  #     parent_docserno = ccnotes_parent_docserno
  #   ) |>
  #   dplyr::select(
  #     -client_number
  #   )

  qol <- clean_form(
    complex_care$complex_care_quality_of_life,
    "qol_"
  ) |>
    dplyr::rename(
      parent_docserno = qol_parent_docserno
    ) |>
    dplyr::select(
      -client_number
    )

  shelter_beds <- clean_form(
    complex_care$complex_care_shelter_bed,
    "shelter_bed_"
  ) |>
    dplyr::rename(
      parent_docserno = shelter_bed_parent_docserno
    ) |>
    dplyr::select(
      -client_number
    )

  # Drop docserno from parent event forms to avoid suffix collisions (.x/.y) due
  # to duplication when joining with pathclient. Pathclient is the authoritative
  # source of docserno values.
  roster <- clean_form(
    complex_care$complex_care_roster,
    "roster_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )

  benchmarks <- clean_form(
    complex_care$complex_care_mercy_beacn_benchmarks,
    "benchmarks_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )

  pfp_discharge <- clean_form(
    complex_care$complex_care_pfp_discharge,
    "pfp_discharge_"
  ) |>
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )

  ccnotes <- clean_form(
    complex_care$complex_care_clinical_notes,
    "ccnotes_"
  ) |> 
    dplyr::select(
      -tidyselect::ends_with(
        "docserno"
      )
    )
  
  # Start with pivoted pathclient
  pc <- complex_care$pathclient

  # Invariant: parent_map must contain exactly one row per (client_number,
  # tiedenrollment, parent_docserno). If this is violated, SCD collapse will
  # fail. The result is a long form tibble
  parent_map <- pc |>
    dplyr::select(
      client_number,
      tiedenrollment,
      roster_docserno,
      pfp_metrics_docserno,
      pfp_discharge_docserno
    ) |>
    tidyr::pivot_longer(
      cols = tidyselect::ends_with(
        "docserno"
      ),
      names_to = "event",
      values_to = "parent_docserno"
    ) |>
    dplyr::filter(
      !is.na(
        parent_docserno
      )
    )

  # Helper: collapse SCD table to one row per enrollment
  collapse_scd <- function(
    scd_tbl
  ) {
    parent_map |>
      dplyr::left_join(
        scd_tbl,
        by = "parent_docserno"
      ) |>
      dplyr::arrange(
        client_number,
        tiedenrollment
      ) |>
      dplyr::group_by(
        client_number,
        tiedenrollment
      ) |>
      dplyr::summarise(
        dplyr::across(
          .cols = -c(
            parent_docserno,
            event
          ),
          .fns  = ~ first(
            na.omit(
              .x
            )
          )
        ),
        .groups = "drop"
      )
  }

  # Diagnostic: payor_one shows which active payor source SCD row was selected
  # for each enrollment. Useful for debugging missing or stale SCD values.
  payor_one   <- collapse_scd(
    payor
  )

  # Diagnostic: housing_one shows which active housing status SCD row was
  # selected for each enrollment. Useful for debugging missing or stale SCD
  # values.
  housing_one <- collapse_scd(
    housing
  )

  # # Diagnostic: intake_one shows which committee notes SCD row was selected for
  # # each enrollment. Useful for debugging missing or stale SCD values.
  # ccnotes_one  <- collapse_scd(
  #   ccnotes
  # )

  # Diagnostic: intake_one shows which shelter bed SCD row was selected for
  # each enrollment. Useful for debugging missing or stale SCD values.
  shelter_beds_one  <- collapse_scd(
    shelter_beds
  )

  # Diagnostic: intake_one shows which quality of life SCD row was selected for
  # each enrollment. Useful for debugging missing or stale SCD values.
  qol_one  <- collapse_scd(
    qol
  )

  # Start join sequence with joined "authoritative" pathclient
  joined <- pc |>
    # Join SCD "active" summaries once per enrollment
    dplyr::left_join(
      payor_one,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      housing_one,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    ) |>

    # # Join SCD "committee notes" once per enrollment
    # dplyr::left_join(
    #   ccnotes_one,
    #   by = c(
    #     "client_number",
    #     "tiedenrollment"
    #   )
    # ) |>

    # Join SCD "shelter bed" once per enrollment
    dplyr::left_join(
      shelter_beds_one,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    ) |>

    # Join SCD "quality of life" once per enrollment
    dplyr::left_join(
      qol_one,
      by = c(
        "client_number",
        "tiedenrollment"
      )
    ) |>

    # Join event forms by enrollment
    dplyr::left_join(
      roster,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      benchmarks,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |>
    dplyr::left_join(
      pfp_discharge,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    ) |> 
    dplyr::left_join(
      ccnotes,
      by = join_by(
        "client_number",
        "tiedenrollment"
      )
    )

  # Store the final joined referral flow table
  output <- list(
    scd = list(
      # ccnotes_one  = ccnotes_one,
      shelter_beds_one = shelter_beds_one,
      qol_one = qol_one,
      payor_one   = payor_one,
      housing_one = housing_one
    ),
    parent_map = parent_map,
    transformed = list(
      roster = roster,
      ccnotes = ccnotes,
      benchmarks = benchmarks,
      pfp_discharge = pfp_discharge
    ),
    joined_referral_flow = joined,
    
    ext_mercy_utilization = complex_care$complex_care_ext_mercy_utilization,
    ext_atd_notifications = complex_care$complex_care_ext_atd_notifications,
    ext_atd_watchlist = complex_care$complex_care_ext_atd_watchlist,
    ext_pfp_service_history = complex_care$complex_care_ext_pfp_service_history
  )

  # Return the joined_referral_flow and scd
  return(
    output
  )

}

# ===
# 4. Extract final complex_care_full_data ----
#   - Returns the final wide referral_flow table
#   - Maintained as a semantic wrapper to expose the final wide table as
#     complex_care_full_data
# ===

extract_complex_care_full_data <- function(
  referral_flow
) {
  referral_flow$joined_referral_flow
}

# ===
# 5. complex care ETL entry point ----
#   - Loads analytic_fields metadata
#   - Ingests all complex care extracts using metadata-driven loaders
#   - Applies complex care-specific transformations (e.g., pivoting, referral_flow)
#   - Returns a named list of all complex care data objects
#   - Does not write to disk or modify global env objects as the old ETL code
#       did.
# ===
run_complex_care_etl <- function(
  analytic_fields,
  complex_care_client,
  complex_care_provider_placement,
  complex_care_pathclient,
  complex_care_pathway_docsernos,
  complex_care_roster,
  complex_care_clinical_notes,
  complex_care_mercy_beacn_benchmarks,
  complex_care_pfp_discharge,
  complex_care_quality_of_life,
  complex_care_shelter_beds,
  complex_care_active_payor_source,
  complex_care_all_payor_source,
  complex_care_active_housing,
  complex_care_all_housing,
  complex_care_ext_mercy_utilization,
  complex_care_ext_atd_notifications,
  complex_care_ext_atd_watchlist,
  complex_care_ext_pfp_service_history,
  start_date = NULL,
  end_date = NULL,
  fiscal_system = c(
    "federal",
    "state"
  )
) {

  fiscal_system <- match.arg(
    fiscal_system
  )

  # analytic_fields is passed in by {targets} or default-loaded
  # No internal call to analytic_fields <- load_analytic_fields() is needed

  # print(
  #   "Columns from analytic_fields + field_name placeholder"
  # )
  # print(
  #   names(
  #     analytic_fields
  #   )
  # )

  # ===
  # 1. Raw Ingestion
  # ===
  complex_care_raw <- list(
    complex_care_client = complex_care_client,
    complex_care_provider_placement = complex_care_provider_placement,
    complex_care_pathclient = complex_care_pathclient,
    complex_care_pathway_docsernos = complex_care_pathway_docsernos,
    complex_care_roster = complex_care_roster,
    complex_care_mercy_beacn_benchmarks = complex_care_mercy_beacn_benchmarks,
    complex_care_pfp_discharge = complex_care_pfp_discharge,
    complex_care_quality_of_life = complex_care_quality_of_life,
    complex_care_shelter_beds = complex_care_shelter_beds,
    complex_care_clinical_notes = complex_care_clinical_notes,
    complex_care_active_payor_source = complex_care_active_payor_source,
    complex_care_all_payor_source = complex_care_all_payor_source,
    complex_care_active_housing = complex_care_active_housing,
    complex_care_all_housing = complex_care_all_housing,
    complex_care_ext_mercy_utilization = complex_care_ext_mercy_utilization,
    complex_care_ext_atd_notifications = complex_care_ext_atd_notifications,
    complex_care_ext_atd_watchlist = complex_care_ext_atd_watchlist,
    complex_care_ext_pfp_service_history = complex_care_ext_pfp_service_history
  )

  # ===
  # 2. Transformations
  # ===
  pathclient <- transform_complex_care_pathclient(
    complex_care_raw
  )
  # Promote pivoted pathclient to authoritative
  complex_care_raw$pathclient <- pathclient$joined_pathclient

  referral_flow <- transform_complex_care_referral_flow(
    complex_care_raw
  )

  full <- extract_complex_care_full_data(
    referral_flow
  )

  # Subsets are optional; only build if dates are supplied
  subsets <- NULL
  if (
    !is.null(
      start_date
    ) && !is.null(
      end_date
    )
  ) {
    subsets <- build_subsets(
      full_data = full,
      start_date = start_date,
      end_date = end_date,
      fiscal_system = fiscal_system
    )
  }

  # ===
  # 3. Return structured object
  # ===
  list(
    raw = complex_care_raw,
    transform  = list(
      pathclient = pathclient,
      referral_flow = referral_flow
    ),
    complex_care_full_data = full,
    subsets = subsets
  )
}