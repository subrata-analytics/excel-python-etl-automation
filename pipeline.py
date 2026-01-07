from src.profile import profile_data
from src.extract import get_config, get_raw_data
from src.utils.helpers import save_profile_report
from src.utils.logger import get_logger



# Extract configurations
pipeline_cfg = get_config("config/pipeline.yaml")
profile_cfg = get_config("config/profile.yaml")

wb_path = pipeline_cfg.get("input", {}).get("file") # Workbook path
ws_name = pipeline_cfg.get("input", {}).get("sheet_name") # Worksheet name

log_path = pipeline_cfg.get("log_files", {}).get("pipeline")

profile_before_path = pipeline_cfg.get("profiling_output", {}).get("before")

# Profile logger
profile_logger = get_logger("profile", log_path)

# Extract raw data
df_raw = get_raw_data(wb_path, ws_name)

# Get profiling report
report = profile_data(df_raw, profile_cfg, profile_logger)

# Save profiling report of the raw data
save_profile_report(report, profile_before_path)
