import logging

from src.brostar_api_requests.brostar_api_requests import (
    send_gldaddition_for_vitens_location,
)


def main():
    send_gldaddition_for_vitens_location("46B-0735001", "62254944", "1103")
    # file_path = r"C:\Users\steven.hosper\Downloads\duplicates_ids.xlsx"
    # correct_bulk_gld(file_path)
    # retry_upload_task()
    # print(total_events_delivered())

    # Create GLD's Vitens
    # file_path = r"C:\Users\steven.hosper\Downloads\overview_gmn_vitens_waterschap.xlsx"
    # create_bulk_gld(excel_file=file_path)

    # Ingest GLD Vitens
    # file_path = r"C:\Users\steven.hosper\Downloads\overview_gmn_vitens_v2.xlsx"
    # df = pl.read_excel(file_path)
    # df = df.filter(pl.col("bro_id").is_not_null())
    # for row in df.iter_rows(named=True):
    #     gld_to_lizard(row['objectIdAccountableParty'], row['bro_id'])

    # delete_invalid_upload_tasks()

    # Tholen Correction!!!
    # bulk_gmw_correction_request(20166109)

    # Scheldestromen deliveries
    # file_path = r"C:\Users\steven.hosper\Downloads\20250709_ScheldestromenImportExcel_test.xlsx"
    # bulk_gmw_construction_request(file_path, "51640813")

    # BrabantWater corrections
    # file_path = r"C:\Users\steven.hosper\Downloads\BROLab_ImportExcel.xlsx"
    # bulk_gmw_construction_request(file_path, "17278718")

    # Gelderland Corrections
    # file_path = r"C:\Users\steven.hosper\Downloads\freatische_filter_gmw_ids.xlsx"
    # bulk_gmw_tubenumber_correction_request(file_path, "51468751")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
