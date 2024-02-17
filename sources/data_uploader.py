import json
import pandas as pd
import requests
import urllib3

import get_data
from config import PROD_API_KEY, TEST_API_KEY
from api_handler import (
    convert_to_timestamp,
    create_payload,
    make_api_request,
    handle_response_errors,
    make_retry_api_request,
)

def main():
    df = get_data.get_full_data(refresh_time_unit="hour", refresh_time_value="1")

    # API endpoint for batch updates
    api_endpoint = "endpoint_goes_here"
    certificate_path = "ssl_certificate/hubspot_cert.pem"

    # Set up headers with API key
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {PROD_API_KEY}",
    }

    # Define mapping of column names to property names
    column_to_property_mapping = {
        "column_name": "property_name",
        # Add more properties if needed
    }

    # Prepare data for the API request
    payload = []

    # Create API payload
    for _, row in df.iterrows():
        contact_data = create_payload(row, column_to_property_mapping)
        payload.append(contact_data)

    # Define the batch size
    batch_size = min(99, len(payload))

    # Split the payload into batches
    for i in range(0, len(payload), batch_size):
        batch_payload = payload[i : i + batch_size]
        make_api_request(api_endpoint, batch_payload, headers, certificate_path)


if __name__ == "__main__":
    main()
