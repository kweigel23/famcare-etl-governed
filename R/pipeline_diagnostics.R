run_diagnostics <- function(
  program = "epicc"
) {
 library(targets)
 
 # Load metadata
 analytic_fields <- tar_read(
  "analytic_fields"
 )
 
 # Define program-specific configuration list
 program_config <- list(
  epicc = list(
   full_target = "epicc_full_data",
   form_prefixes = c(
    "ref_",
    "ic_",
    "twow_",
    "thirtyd_",
    "threem_",
    "sixm_",
    "reengage_"
   ),
   referral_flow_target = "epicc_referral_flow",
   scd_prefixes = c(
    "intake_",
    "payor_",
    "housing_"
   ),
   form_docserno_cols = c(
    "ref_docserno",
    "ic_docserno",
    "twow_docserno",
    "thirtyd_docserno",
    "threem_docserno",
    "sixm_docserno"
   ),
   scd_raw_targets = c(
    "epicc_active_intake_raw",
    "epicc_active_payor_source_raw",
    "epicc_active_housing_raw"
   )
  ),
  complex_care = list(
   full_target = "complex_care_full_data",
   form_prefixes = c(
    "roster_",
    "ccnotes_",
    "benchmarks_",
    "pfp_discharge_"
   ),
   referral_flow_target = "complex_care_referral_flow",
   scd_prefixes = c(
    "housing_",
    "payor_",
    "shelter_beds_",
    "qol_"
   ),
   form_docserno_cols = c(
    "roster_docserno",
    "pfp_metrics_docserno",
    "pfp_discharge_docserno"
   ),
   scd_raw_targets = c(
    "complex_care_active_housing_raw",
    "complex_care_active_payor_source_raw",
    "complex_care_shelter_beds_raw",
    "complex_care_quality_of_life_raw"
   )
  ),
  bcr = list(
   full_target = "bcr_full_data",
   form_prefixes = c(
    "ref_",
    "ic_",
    "rp_",
    "ccs_"
   ),
   referral_flow_target = "bcr_referral_flow",
   scd_prefixes = c(
    "housing_",
    "payor_",
    "pc_"
   ),
   form_docserno_cols = c(
    "ref_docserno",
    "ic_docserno",
    "rp_docserno"
   ),
   scd_raw_targets = c(
    "bcr_active_housing_raw",
    "bcr_active_payor_source_raw",
    "bcr_presenting_concerns_raw"
   )
  ),
  ere = list(
   full_target = "ere_full_data",
   form_prefixes = c(
    "ref_",
    "hosp_visit_",
    "ihna_",
    "threem_",
    "sixm_",
    "bhs_"
   ),
   referral_flow_target = "ere_referral_flow",
   scd_prefixes = c(
    "housing_",
    "payor_",
    "client_needs_"
   ),
   form_docserno_cols = c(
    "ref_docserno",
    "hosp_visit_docserno",
    "ihna_docserno",
    "threem_docserno",
    "sixm_docserno",
    "bhs_docserno"
   ),
   scd_raw_targets = c(
    "ere_active_housing_raw",
    "ere_active_payor_source_raw",
    "ere_client_needs_raw"
   )
  ),
  yere = list(
   full_target = "yere_full_data",
   form_prefixes = c(
    "ref_",
    "hosp_visit_",
    "ia_",
    "thirtyd_",
    "threem_",
    "sixm_",
    "bhs_"
   ),
   referral_flow_target = "yere_referral_flow",
   scd_prefixes = c(
    "housing_",
    "payor_",
    "client_needs_",
    "caregiver_needs_"
   ),
   form_docserno_cols = c(
    "ref_docserno",
    "hosp_visit_docserno",
    "ia_docserno",
    "thirtyd_docserno",
    "threem_docserno",
    "sixm_docserno",
    "bhs_docserno"
   ),
   scd_raw_targets = c(
    "yere_active_housing_raw",
    "yere_active_payor_source_raw",
    "yere_client_needs_raw",
    "yere_caregiver_needs_raw"
   )
  )
 )
 
 # Add allowed_prefixes dynamically using imap(). The point is that the
 # <program>_full_data tibble will include some legitimate prefixes that will not
 # be references to forms (form_prefixes). Without mapping these additional
 # allowed prefixes, the diagnostic checking will be too strict and will
 # repeatedly flag with false positives.
 program_config <- purrr::imap(
  program_config,
  function(
  cfg,
  name
  ) {
   cfg$allowed_prefixes <- c(
    cfg$form_prefixes,
    "client_",
    "enrollment_",
    "tiedenrollment",
    "pwy_"
   )
   cfg
  }
 )
 
 # Create a look up table of <program>_full_data targets to pass to {targets}.
 full_targets <- c(
  epicc = "epicc_full_data",
  bcr   = "bcr_full_data",
  ere   = "ere_full_data",
  yere  = "yere_full_data",
  complex_care = "complex_care_full_data"
 )

 # Load program-specific full data
 target_name <- full_targets[[program]]
 
 tar_load(
  target_name
  )
 
 full <- get(
  target_name
 )

 # load program-specific form prefixes
 form_prefixes <- program_config[[program]]$form_prefixes
 
 # load referral_flow dynamically
 referral_flow_target <- program_config[[program]]$referral_flow_target
 
 tar_load(
  referral_flow_target
 )
 
 referral_flow <- get(
  referral_flow_target
 )
 
 # Load raw objects
 load_raw_objects <- function(
  program
 ) {
  raw_targets <- program_config[[program]]$scd_raw_targets
  
  purrr::map(
   raw_targets,
   function(
  x
  ) {
   tar_load(
    x
    )
   get(
    x
    )
  } )|> 
   purrr::set_names(
    raw_targets
   )
 }
 
 scd_tables <- load_raw_objects(
  program
 )
 
 # Add a message helper function
 announce <- function(
  label,
  expr
  ) {
  message(
   "• ",
   label,
   " ... ",
   appendLF = FALSE
   )
  force(
   expr
   )
  message(
   crayon::green(
    "✔ passed"
    )
   )
 }
 
 # Run diagnostics
 
 # 1. Column hygiene
 announce(
  "Column prefix hygiene",
  check_column_prefixes(
   full,
   allowed_prefixes = program_config[[program]]$allowed_prefixes
  )
 )
 
 announce(
  "Duplicate column check",
  check_no_duplicate_columns(
   full
  )
 )
 
 # 2. Form completion
 announce(
  "Form completion summary",
  summarise_form_completion(
   full,
   form_prefixes
  )
 )
 
 # 3. SCD coverage
 if (
  length(
   program_config[[program]]$scd_parent_cols
   ) > 0
  ) {
  announce(
   "SCD coverage",
   summarise_scd_coverage(
    full,
    program_config[[program]]$scd_parent_cols
    )
   )
 }
 
 # 4. Parent/child alignment
 if (
  length(
   program_config[[program]]$form_docserno_cols
   ) > 0
  ) {
  announce(
   "Parent/child alignment",
   check_parent_form_alignment(
    full,
    program_config[[program]]$scd_parent_cols,
    program_config[[program]]$form_docserno_cols
    )
   )
 }
 
 # 5. SCD parent matches
 if (
  length(
   program_config[[program]]$form_docserno_cols
   ) > 0
  ) {
  announce(
   "SCD parent matches",
   check_scd_parent_matches(
    referral_flow,
    scd_tables,
    program_config[[program]]$form_docserno_cols
   )
  )
 }

}
