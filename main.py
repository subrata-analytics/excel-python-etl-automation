import argparse
from etl.etl_pipeline import run_etl
from etl.profile import show_profile
from utils.generate_messy_excel_file import (
    generate_data, 
    inspect_generated_data
)


def main():
    parser = argparse.ArgumentParser(description="Sales Data Tools")
    subparsers = parser.add_subparsers(dest="command")

    # profile command
    subparsers.add_parser("profile", help="Run profiler")

    # generate subcommand
    gen = subparsers.add_parser("generate", help="Generate random sales data")
    gen.add_argument(
        "--rows", 
        type=int,
        default=10,
        help="Number of rows")
    gen.add_argument(
        "--test",
        action="store_true",
        help="Test generated data in Excel file."
    )

    # etl subcommand
    subparsers.add_parser("etl", help="Run ETL pipeline")

    args = parser.parse_args()

    if args.command == "profile":
        show_profile()

    elif args.command == "generate":
        if args.rows <= 0:
            raise parser.error("rows must be a positive integer")
        
        generate_data(args.rows)
        
        if args.test:
            inspect_generated_data()
    
    elif args.command == "etl":
        run_etl()
    else:
        parser.error(
            "You must specify a command: generate [--rows] or etl"
        )


if __name__ == "__main__":
    main()