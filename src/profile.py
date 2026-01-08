import numpy as np
import pandas as pd
from logging import Logger
from scipy.stats import zscore


def profile_data(df: pd.DataFrame,
                 config: dict, 
                 logger: Logger) -> dict:
    """
    Profiles data in the df based on the profiling 
    configuration config and returns a report
    
    :param df: Data as a dataframe
    :type df: pd.DataFrame
    :param config: Profiling configuration
    :type config: dict
    :param logger: Logger for profiling
    :type logger: logging.Logger
    :return: Report on profiling
    :rtype: dict[Any, Any]
    """

    logger.info("Starting data profiling...")

    report = {}

    # Standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # Extract profile configuration
    profile_enabled = config["profile"]["enabled"]
    columns = config["columns"]
    metrics_cfg = config["metrics"]
    outliers_cfg = config["outliers"]
    quality_cfg = config["quality_score"]

    if not profile_enabled:
        return {"message": "Profiling disabled in profile.yaml"}
    
    # Restrict DF to configured columns (if they exist)
    df = df[[col for col in columns if col in df.columns]]

    # Analyze metrics
    report["metrics"] = {}
    if metrics_cfg["missing_values"]:
        missing_values = df.isna().sum().to_dict()
        report["metrics"]["missing_values"] = missing_values
        logger.info(
            f"Computed missing vlaues: {len(missing_values) > 0}"
        )
        
    if metrics_cfg["missing_percent"]:
        missing_percent = df.isna().mean().mul(100).to_dict()
        report["metrics"]["missing_percent"] = (
            missing_percent
        )
        logger.info(
            f"Computed missing percentage: {len(missing_percent) > 0}"
        )
    
    if metrics_cfg["unique_values"]:
        report["metrics"]["unique_values"] = df.isna().nunique().to_dict()
        logger.info("Computed unique values")
    
    if metrics_cfg["data_types"]:
        report["metrics"]["data_types"] = df.dtypes.astype(str).to_dict()
        logger.info("Computed data types")
    
    if metrics_cfg["numeric_summary"]:
        numeric_cols = df.select_dtypes(include="number").columns
        report["metrics"]["numeric_summary"] = (
            df[numeric_cols].describe().to_dict()
        )
        logger.info("Summarized numerical columns")
    
    if metrics_cfg["categorical_distribution"]:
        categorical_cols = df.select_dtypes(include="object").columns
        report["metrics"]["categorical_distribution"] = {
            col: df[col].value_counts().to_dict() for col in categorical_cols
        }
        logger.info("Summarized categorical data")
    
    if metrics_cfg["sample_rows"]:
        n = metrics_cfg["sample_rows"]
        report["metrics"]["sample_rows"] = df.head(n).to_dict(orient="records")
        logger.info("Summarized metrics")
    
    # Detect outliers
    outlier_report = {}

    if outliers_cfg["enabled"]:
        method = outliers_cfg["method"]
        threshold = outliers_cfg["threshold"]
        numeric_cols = outliers_cfg["numeric_columns"]

        for col in numeric_cols:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                if method == "zscore":
                    z_scores = np.abs(zscore(df[col].dropna()))
                    outlier_idx = df[col].dropna().index[z_scores > threshold]
                    outlier_report[col] = {
                        "count": len(outlier_idx),
                        "indices": outlier_idx.tolist()
                    }
                    
        report["outliers"] = outlier_report
        logger.info("Computed outliers")

    # Quality Score
    if quality_cfg["enabled"]:
        weights = quality_cfg["weights"]

        # Missing ratio
        missing_ratio = df.isna().mean().mean()

        # Invalid values (negative numeric values)
        numeric_cols = df.select_dtypes(include="number").columns
        invalid_ratio = (
            (df[numeric_cols] < 0).sum().sum() / df[numeric_cols].size
            if len(numeric_cols) > 0 else 0
        )

        # Duplicate ratio
        duplicate_ratio = df.duplicated().mean()

        # Anomaly ratio
        anomaly_ratio = (
            sum(v["count"] for v in outlier_report.values()) / len(df)
            if len(df) > 0 else 0
        )

        # Weighted score
        score = (
            (1 - missing_ratio) * weights["missing_values"] +
            (1 - invalid_ratio) * weights["invalid_values"] +
            (1 - duplicate_ratio) * weights["duplicates"] +
            (1 - anomaly_ratio) * weights["anomalies"]
        )

        report["quality_score"] = np.round(score, 4).tolist()
        logger.info("Computed quality score")
    
    logger.info(f"Profiling completed successfully.")
    return report