# api_handler.py
import json
import requests
import pandas as pd

def convert_to_timestamp(value):
    try:
        # Attempt to convert to datetime and set time to midnight in UTC
        datetime_value = pd.to_datetime(value, utc=True).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        if pd.notna(datetime_value):
            # Ensure the resulting timestamp is a valid long integer
            return int(datetime_value.timestamp() * 1000)
        else:
            return None
    except (TypeError, ValueError):
        # Handle conversion errors or null values
        return None

def create_payload(row, column_to_property_mapping):
    contact_data = {"email": str(row["Email"]).strip(), "properties": []}

    for column, property_name in column_to_property_mapping.items():
        value = row[column]
        if pd.isna(value):
            contact_data["properties"].append(
                {"property": property_name, "value": None}
            )
        else:
            value = convert_to_timestamp(value) if "date" in property_name.lower() else str(value)
            contact_data["properties"].append({"property": property_name, "value": value})

    return contact_data

def make_api_request(api_endpoint, batch_payload, headers, certificate_path):
    try:
        response = requests.post(
            api_endpoint, json=batch_payload, headers=headers, verify=certificate_path
        )
        
        handle_response_errors(response, batch_payload)

    except Exception as e:
        print(f"Error making API request: {str(e)}")

def handle_response_errors(response, batch_payload):
    if response.status_code == 202:
        print("Request has been accepted for processing.")
    elif response.text:
        try:
            response_data = response.json()
            if response_data.get("status") == "error":
                # Handle errors in the response
                error_messages = response_data.get("failureMessages", [])
                invalid_emails = response_data.get("invalidEmails", [])

                for error_message in error_messages:
                    index = error_message.get("index")
                    validation_result = error_message.get("propertyValidationResult", {})
                    is_valid = validation_result.get("isValid", True)
                    if not is_valid:
                        invalid_emails.append(batch_payload[index]["email"])

                valid_records = [
                    record for record in batch_payload if record["email"] not in invalid_emails
                ]

                if valid_records:
                    print("Continuing with valid records.")
                    make_retry_api_request(api_endpoint, valid_records, headers, certificate_path)
                else:
                    print("All emails in the batch are invalid.")
        
        except json.JSONDecodeError:
            print(f"Error decoding JSON response: {response.text}")

    else:
        print("Empty response received from the API -> No error -> Data is posted")

def make_retry_api_request(api_endpoint, valid_records, headers, certificate_path):
    try:
        response_retry = requests.post(
            api_endpoint, json=valid_records, headers=headers, verify=certificate_path
        )

        if response_retry.status_code == 200:
            print("Retry - Batch successfully posted to the API.")
        elif response_retry.status_code == 202:
            print("Retry - Request has been accepted for processing.")
        else:
            print("Retry - Error: {response_retry.status_code} - {response_retry.text}")

    except Exception as e:
        print(f"Error making retry API request: {str(e)}")
