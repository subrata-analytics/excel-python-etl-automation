import json


def save_profile_report(report: dict, json_path: str):
    with open(json_path, "w") as f:
        json.dump(report, f, indent=4)
