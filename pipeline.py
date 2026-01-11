# =============================================================================
# Pipeline
# =============================================================================
from src.profile import profile_data
from src.normalize import normalize_data
from src.curate import curate_data

from src.extract import get_config, get_raw_data

from src.utils.helpers import save_profile_report
from src.utils.logger import get_logger
from src.utils.lineage import LineageWriter

from src.normalize import normalize_data


# Extract configurations
pipeline_cfg = get_config("config/pipeline.yaml")
profile_cfg = get_config("config/profile.yaml")
curation_cfg = pipeline_cfg["curation"]

workbook_path = pipeline_cfg["input"]["file"]
worksheet_name = pipeline_cfg["input"]["sheet_name"]

log_path = pipeline_cfg["log_files"]["pipeline"]

lineage_path = pipeline_cfg["lineage"]["output_file"]

profile_path_before = pipeline_cfg["profiling_output"]["before"]
profile_path_after = pipeline_cfg["profiling_output"]["after"]

processed_data_path = pipeline_cfg["output"]["processed"]
curate_data_path = pipeline_cfg["output"]["curated"]

# Profile logger
profile_logger = get_logger("profile", log_path, log_shell=True)

# Normalize logger
normalize_logger = get_logger("normalize", log_path, log_shell=True)

# Curate logger
curation_logger = get_logger("curate", log_path, log_shell=True)

# Lineage Writer
lineage_writer = LineageWriter(lineage_path)


def run_pipeline():
    """
        Run the Extract, Transform, and Load Script in a sequence
    """

    #===========================================================================
    # Extract raw data
    #===========================================================================
    df_raw = get_raw_data(workbook_path, worksheet_name)

    #===========================================================================
    # Profiling
    #===========================================================================
    # Get profiling report of the raw data
    profile_report_before = profile_data(
        df_raw, 
        pipeline_cfg, 
        profile_cfg, 
        profile_logger
    )

    #===========================================================================
    # Transformation
    #===========================================================================
    # Normalize raw data
    df_cleaned = normalize_data(
        df_raw,
        pipeline_cfg,
        normalize_logger,
        lineage_writer
    )

    # Curate cleaned data
    df_curated = curate_data(
        df_cleaned,
        curation_cfg,
        curation_logger
    )

    #===========================================================================
    # Profiling
    #===========================================================================
    # Get profiling report of the cleaned data
    profile_report_after = profile_data(
        df_cleaned, 
        pipeline_cfg, 
        profile_cfg, 
        profile_logger
    )


    #===========================================================================
    # Loading
    #===========================================================================
    # Save profiling report of the raw data
    save_profile_report(profile_report_before, profile_path_before)

    # Save the cleaned data
    df_cleaned.to_csv(processed_data_path, index=False)

    # Save curated data
    df_curated.to_csv(curate_data_path, index=False)


if __name__ == "__main__":
    run_pipeline()