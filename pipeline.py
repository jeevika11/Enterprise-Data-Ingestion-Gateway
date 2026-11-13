
from datetime import datetime, timezone
import math


def validate_payload(payload):
    """Validate incoming ingestion payload."""

    if not isinstance(payload, dict):
        raise ValueError("Payload must be a dictionary.")

    if not payload:
        raise ValueError("Payload cannot be empty.")

    if "records" in payload:
        records = payload["records"]

        if not isinstance(records, list):
            raise ValueError("'records' must be a list.")

        if not records:
            raise ValueError("'records' cannot be empty.")

        for index, record in enumerate(records):
            if not isinstance(record, dict):
                raise ValueError(
                    f"Record {index} must be a dictionary."
                )

    return True


def clean_value(value):
    """Recursively clean values in the payload."""

    if isinstance(value, str):
        return value.strip()

    if isinstance(value, dict):
        return {
            key: clean_value(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [clean_value(item) for item in value]

    return value


def clean_payload(payload):
    """Clean incoming payload data."""

    validate_payload(payload)

    cleaned = clean_value(payload)

    return cleaned


def transform_payload(payload):
    """Transform records and calculate summary statistics."""

    validate_payload(payload)

    transformed = dict(payload)

    records = transformed.get("records", [])

    numeric_total = 0.0
    numeric_count = 0

    for record in records:

        for key, value in record.items():

            if isinstance(value, (int, float)) and not isinstance(value, bool):

                if not math.isfinite(value):
                    raise ValueError(
                        f"Non-finite numeric value in field: {key}"
                    )

                numeric_total += value
                numeric_count += 1

    transformed["metadata"] = {
        "record_count": len(records),
        "numeric_field_count": numeric_count,
        "numeric_total": numeric_total,
        "pipeline_status": "completed"
    }

    return transformed


def run_pipeline(payload):
    """Execute the complete sequential data pipeline."""

    start_time = datetime.now(timezone.utc)

    # Stage 1: Validate
    validate_payload(payload)

    # Stage 2: Clean
    cleaned_data = clean_payload(payload)

    # Stage 3: Transform
    transformed_data = transform_payload(cleaned_data)

    end_time = datetime.now(timezone.utc)

    transformed_data["metadata"]["processed_at"] = (
        end_time.isoformat()
    )

    transformed_data["metadata"]["processing_duration_seconds"] = (
        end_time - start_time
    ).total_seconds()

    return transformed_data


if __name__ == "__main__":

    print("=" * 60)
    print("TASK 15 - DATA PIPELINE TEST")
    print("=" * 60)

    sample_payload = {
        "source": "sales_system",
        "records": [
            {
                "id": 1,
                "name": "  Laptop  ",
                "amount": 50000,
                "quantity": 2
            },
            {
                "id": 2,
                "name": "  Keyboard  ",
                "amount": 2000,
                "quantity": 5
            },
            {
                "id": 3,
                "name": " Mouse ",
                "amount": 1000,
                "quantity": 3
            }
        ]
    }

    # Test 1: Payload validation
    validation_check = validate_payload(sample_payload)

    print("\nPayload validation:",
          "PASS" if validation_check else "FAIL")

    # Test 2: Data cleaning
    cleaned = clean_payload(sample_payload)

    cleaning_check = (
        cleaned["records"][0]["name"] == "Laptop"
        and cleaned["records"][1]["name"] == "Keyboard"
    )

    print("Data cleaning verification:",
          "PASS" if cleaning_check else "FAIL")

    # Test 3: Transformation
    transformed = transform_payload(cleaned)

    transformation_check = (
    transformed["metadata"]["record_count"] == 3
    and transformed["metadata"]["numeric_total"] == 53016
)

    print("Transformation verification:",
          "PASS" if transformation_check else "FAIL")

    # Test 4: Complete pipeline
    result = run_pipeline(sample_payload)

    pipeline_check = (
        result["metadata"]["pipeline_status"] == "completed"
        and result["metadata"]["record_count"] == 3
        and result["records"][0]["name"] == "Laptop"
    )

    print("Complete pipeline verification:",
          "PASS" if pipeline_check else "FAIL")

    # Display output
    print("\nTRANSFORMED DATA")
    print("-" * 60)

    for record in result["records"]:
        print(record)

    print("\nPIPELINE METADATA")
    print(result["metadata"])

    # Final summary
    checks = [
        validation_check,
        cleaning_check,
        transformation_check,
        pipeline_check
    ]

    passed = sum(checks)

    print("\n" + "=" * 60)
    print(f"Tests passed: {passed}/4")
    print(f"Tests failed: {4 - passed}/4")

    print(
        "\nPIPELINE STATUS:",
        "ALL TESTS PASSED" if passed == 4
        else "CHECK FAILED TESTS"
    )

    print("=" * 60)
