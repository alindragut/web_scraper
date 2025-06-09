import csv
import requests
import json
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# Configuration
API_URL = "http://localhost:8000/match"
INPUT_CSV_FILE = "data/API-input-sample.csv"
REQUEST_TIMEOUT_SECONDS = 15

def run_api_tests():
    """
    Reads company data from a CSV, tests the /match API endpoint
    and reports the match rate.
    """
    successful_matches = 0
    failed_matches = 0
    total_requests = 0

    log.info(f"Reading test data from: {INPUT_CSV_FILE}")

    try:
        with open(INPUT_CSV_FILE, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            test_cases = list(reader)
            total_requests = len(test_cases)

            for i, row in enumerate(test_cases):
                payload = {
                    "name": row.get("input name"),
                    "phone": row.get("input phone"),
                    "website": row.get("input website"),
                    "input_facebook": row.get("input_facebook")
                }
                
                log.info(f"[{i + 1}/{total_requests}] Testing with payload: {payload}")

                try:
                    response = requests.post(
                        API_URL, 
                        json=payload, 
                        timeout=REQUEST_TIMEOUT_SECONDS
                    )

                    if response.status_code == 200:
                        successful_matches += 1
                        matched_company = response.json().get("company_name", "N/A")
                        log.info(f"Matched with '{matched_company}': {response.json()}")
                    elif response.status_code == 404:
                        # A 404 is an expected failure, so we log it as a WARNING.
                        failed_matches += 1
                        log.warning(f"No match found in the database.")
                    else:
                        failed_matches += 1
                        log.error(f"({response.status_code}): {response.text}")

                except requests.exceptions.RequestException as e:
                    failed_matches += 1
                    log.critical(f"Could not connect to the API. {e}")
                
                time.sleep(0.1)

    except FileNotFoundError:
        log.critical(f"The input file '{INPUT_CSV_FILE}' was not found.")
        return
    except Exception as e:
        log.critical(f"An unexpected error occurred during the test run.", exc_info=True)
        return

    log.info("Match Rate Report")
    log.info(f"Total Test Cases:      {total_requests}")
    log.info(f"Successful Matches:    {successful_matches}")
    log.info(f"Failed/No Matches:     {failed_matches}")

    if total_requests > 0:
        match_rate = (successful_matches / total_requests) * 100
        log.info(f"Overall Match Rate:  {match_rate:.2f}%")
    else:
        log.warning("No test cases were run.")

if __name__ == "__main__":
    run_api_tests()