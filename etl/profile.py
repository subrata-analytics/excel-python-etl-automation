import pandas as pd


def show_profile():
    # Profiling raw data in Excel file
    df_raw = pd.read_excel(
        "data/raw/sales_transactions_2024.xlsx",
        sheet_name="SalesData",
        engine="openpyxl"
    )
    profile_raw = get_profile(df_raw, "Raw Sales Data")
    print(profile_raw)

    # Profiling cleaned data in csv file
    df_cleaned = pd.read_csv(
        "data/processed/sales_cleaned.csv",
    )
    profile_cleaned = get_profile(df_cleaned, "Cleaned Sales Data")
    print(profile_cleaned)



def get_profile(df, name="Dataset"):
    print(f"\n=== PROFILE: {name} ===")

    profile = {}

    profile["shape"] = df.shape
    profile["dtypes"] = df.dtypes.astype(str).to_dict()
    profile["missing_count"] = df.isna().sum().to_dict()
    profile["missing_percent"] = (df.isna().mean() * 100).round(2).to_dict()
    profile["unique_values"] = df.nunique().to_dict()

    # Numeric summary
    numeric_cols = df.select_dtypes(include=["float", "int"]).columns
    profile["numeric_summary"] = df[numeric_cols].describe().to_dict()

    # Categorical summary
    categorical_cols = df.select_dtypes(include=["object"]).columns
    profile["categorical_top_values"] = {
        col: df[col].value_counts(dropna=False).head(5).to_dict()
        for col in categorical_cols
    }

    # Print clean summary
    print(f"Rows: {profile['shape'][0]}, Columns: {profile['shape'][1]}")
    print("\nMissing Values:")
    for col, val in profile["missing_count"].items():
        print(f"  {col}: {val} ({profile['missing_percent'][col]}%)")

    print("\nUnique Values:")
    for col, val in profile["unique_values"].items():
        print(f"  {col}: {val}")

    print("\nTop Categorical Values:")
    for col, vals in profile["categorical_top_values"].items():
        print(f"  {col}: {vals}")

    return profile