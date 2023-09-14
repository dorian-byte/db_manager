import logging
import re

class Validator:
    @staticmethod
    def is_valid_email(email):
        pattern = r"[^@]+@[^@]+\.[^@]+"
        return re.match(pattern, email) is not None

    @staticmethod
    def is_valid_datetime(dt_str):
        # this matches the date, "T", "Z"(optional) or timezone offset (+hh:mm or -hh:mm)
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})?$"
        return re.match(pattern, dt_str) is not None

    @staticmethod
    def validate_data_against_schema(data, schema):
        for row in data:
            row_dict = dict(
                row
            )  # assuming each row is a dictionary, and adjust if not

            for key, definition in schema["properties"].items():
                if key not in row_dict and key in schema["required"]:
                    logging.error(f"'{key}' is required but missing from data.")
                    return False

                value = row_dict.get(key)

                # this checks numeric input types
                expected_type = definition["type"]
                if expected_type == "integer" and not isinstance(value, int):
                    logging.error(
                        f"Validation error for '{key}'. Expected an integer, got {type(value)}"
                    )
                    return False
                if expected_type == 'number' and (not isinstance(value, float) and not isinstance(value, int)):
                    logging.error(
                        f"Validation error for '{key}'. Expected a number, got {type(value)}"
                    )
                    return False
                elif expected_type == "string" and not isinstance(value, str):
                    logging.error(
                        f"Validation error for '{key}'. Expected a string, got {type(value)}"
                    )
                    return False
                elif expected_type == "boolean" and not isinstance(value, bool):
                    logging.error(
                        f"Validation error for '{key}'. Expected a boolean, got {type(value)}"
                    )
                    return False

                # this checks email and date-time formats
                format_ = definition.get("format")
                if format_ == "email" and not Validator.is_valid_email(value):
                    logging.error(
                        f"Validation error for '{key}'. Invalid email format."
                    )
                    return False
                elif format_ == "date-time" and not Validator.is_valid_datetime(value):
                    print("-----------")
                    print("wrong date")
                    print(value)
                    print(Validator.is_valid_datetime(value))
                    print("-----------")
                    logging.error(
                        f"Validation error for '{key}'. Invalid date-time format."
                    )
                    return False
        return True
