import logging

from src.brostar_api_requests.brostar_api_requests import (
    delete_invalid_upload_tasks,
)


def main():
    # file_path = r"C:\Users\steven.hosper\Downloads\duplicates_ids.xlsx"
    # correct_bulk_gld(file_path)

    delete_invalid_upload_tasks()

    # file_path = r"C:\Users\steven.hosper\Desktop\PythonPackages\BrostarAPI\20250425_move_wells.xlsx"

    # BrabantWater corrections
    # file_path = r"C:\Users\steven.hosper\Downloads\BROLab_ImportExcel.xlsx"
    # bulk_gmw_construction_request(file_path, "17278718")

    # Gelderland Corrections
    # file_path = r"C:\Users\steven.hosper\Downloads\freatische_filter_gmw_ids.xlsx"
    # bulk_gmw_tubenumber_correction_request(file_path, "51468751")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
