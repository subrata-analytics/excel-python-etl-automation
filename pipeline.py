from src.profile import profile_data
from src.extract import get_config, get_raw_data
from src.utils.helpers import save_profile_report
from src.utils.logger import get_logger



# Extract configurations
pipeline_cfg = get_config("config/pipeline.yaml")
profile_cfg = get_config("config/profile.yaml")

workbook_path = pipeline_cfg["input"]["file"]
worksheet_name = pipeline_cfg["input"]["sheet_name"]

log_path = pipeline_cfg["log_files"]["pipeline"]

profile_path_before = pipeline_cfg["profiling_output"]["before"]
profile_path_after = pipeline_cfg["profiling_output"]["after"]

# Profile logger
profile_logger = get_logger("profile", log_path, log_shell=True)

# Extract raw data
df_raw = get_raw_data(workbook_path, worksheet_name)

# Get profiling report
report = profile_data(df_raw, pipeline_cfg, profile_cfg, profile_logger)

# Save profiling report of the raw data
save_profile_report(report, profile_path_before)
