import csv
import glob
import logging
import os

from dateutil import parser
from database_manager import DatabaseManager
from validator import Validator


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

EXAMPLE_SCHEMA = {
    "title": "User",
    "type": "object",
    "properties": {
        "id": {"type": "integer", "description": "Unique identifier for the user."},
        "username": {"type": "string", "description": "The user's login username."},
        "email": {
            "type": "string",
            "format": "email",
            "description": "The user's email address.",
        },
        "is_active": {
            "type": "boolean",
            "description": "Whether the user's account is active.",
        },
        "user_role": {
            "type": "string",
            "enum": ["admin", "user"],
            "description": "The user's role.",
        },
        "created_at": {
            "type": "string",
            "format": "date-time",
            "description": "The date and time when the user account was created.",
        },
    },
    "required": ["id", "username", "email"],
}


class CSVProcessor:
    TRUE_VALUES = ["true", "True", "TRUE", "1", 1, True, "t"]
    FALSE_VALUES = ["false", "False", "FALSE", "0", 0, False, "f"]
    NULL_VALUES = ["NaN", "null", None, ""]
    SCHEMA_PROCESSING_MAP = {
        "integer": "process_numeric",
        "string": "process_generic_cell",
        "boolean": "process_boolean",
    }

    def __init__(
        self,
        csv_file_path=None,
        db_manager=None,
        db_config=None,
        table_name=None,
        schema=EXAMPLE_SCHEMA,
        use_zero_for_null_numerics=False,
        strict_mode=False,
        csv_has_header=True,
    ):
        # if csv_file_path is None, search for the first .csv in the current directory
        if csv_file_path is None:
            csv_files = glob.glob("./*.csv")
            if not csv_files:
                raise ValueError("No CSV files found in the current directory.")
            csv_file_path = csv_files[0]

        # if db_config is not provided, use default values
        if db_config is None:
            db_config = {
                "dbname": "postgres",
                "user": "postgres",
                "password": "postgres",
                "host": "localhost",
                "port": "5432",
            }
        if not table_name:
            table_name = os.path.basename(csv_file_path).split(".")[0]

        self.csv_file_path = csv_file_path
        self.db_config = db_config
        self.table_name = table_name
        self.schema = schema
        self.strict_mode = strict_mode
        self.use_zero_for_null_numerics = use_zero_for_null_numerics
        self.csv_has_header = csv_has_header
        self.db_manager = db_manager or DatabaseManager(db_config)

    def process_datetime(self, datetime_str: str) -> str:
        try:
            # try parsing input to return an ISO format
            dt = parser.parse(datetime_str)
            return dt.isoformat()
        except Exception as e:
            logging.error(f"Error parsing datetime: {datetime_str}. Error: {e}")
            if self.strict_mode:
                raise
            return None

    def process_generic_cell(self, cell, field_type="string"):
        if not cell:
            if self.strict_mode:
                logging.error("Empty cell found.")
                raise ValueError("Empty cell found.")
            else:
                if field_type == "string":
                    return ""
                elif field_type == "number":
                    return None if not self.use_zero_for_null_numerics else 0
                elif field_type == "integer":
                    return None if not self.use_zero_for_null_numerics else 0
                elif field_type == "boolean":
                    return False
                elif field_type == "datetime":
                    return None
                else:
                    return None
        return cell

    def process_numeric(self, cell):
        if cell in self.NULL_VALUES:
            if self.strict_mode:
                logging.error("Cell should be numeric.")
                raise ValueError("Cell should be numeric.")
            return None if not self.use_zero_for_null_numerics else 0
        if int(cell) == float(cell):
            return int(cell)
        return float(cell)

    def process_boolean(self, cell):
        if cell in self.TRUE_VALUES:
            return True
        elif cell in self.FALSE_VALUES:
            return False
        else:
            if self.strict_mode:
                logging.error("Cell should be boolean.")
                raise ValueError("Cell should be boolean.")
            return None

    def process_enum(self, cell, enum_values):
        if cell in enum_values:
            return cell

        if self.strict_mode:
            error_message = (
                f"Unexpected enum value: {cell}. Allowed values: {enum_values}."
            )
            logging.error(error_message)
            raise ValueError(error_message)

        return None

    def seems_to_be_header(self, row):
        expected_columns = list(self.schema["properties"].keys())
        return set(row) == set(expected_columns)

    def insert_into_postgres(self, data):
        if self.db_manager:
            self.db_manager.insert_data(self.table_name, data, self.schema)
        else:
            logging.error("Database manager not provided or initialized.")

    def process_csv(self):
        """
        This method processes the CSV file and inserts the data into the database.
        """
        processed_data = []
        row_number = 0

        with open(self.csv_file_path, "r") as file:
            reader = csv.reader(file)

            first_row = next(reader)
            has_header = self.csv_has_header or self.seems_to_be_header(first_row)
            if not has_header:
                file.seek(0)  # reset file pointer if there's no header

            for row in reader:
                row_number += 1

                # process each cell in the row according to the schema, then save each row 
                processed_row = []
                for column, (key, value) in zip(row, self.schema["properties"].items()):
                    is_enum = value.get("enum")
                    is_datetime = value.get("format") == "date-time"

                    if is_enum:
                        processed_value = self.process_enum(column, is_enum)
                    elif is_datetime:
                        processed_value = self.process_datetime(column)
                    else:
                        data_type = value.get("type")
                        processing_func_name = self.SCHEMA_PROCESSING_MAP.get(data_type)

                        if not processing_func_name:
                            logging.error(
                                f"No processing function found for data type {data_type} at row {row_number}"
                            )
                            if self.strict_mode:
                                return
                            else:
                                continue

                        processing_func = getattr(self, processing_func_name)
                        processed_value = processing_func(column)

                    processed_row.append(processed_value)

                # validating the processed row against the schema
                row_dict = dict(zip(self.schema["properties"].keys(), processed_row))
                if not Validator.validate_data_against_schema([row_dict], self.schema):
                    logging.error(f"Data validation failed for row {row_number}: {row}")
                    if self.strict_mode:
                        return

                processed_data.append(tuple(processed_row))

        self.insert_into_postgres(processed_data)


if __name__ == "__main__":
    """
    This is an example of how to use the CSVProcessor class.
    """
    db_config = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": "5432",
    }
    db_manager = DatabaseManager(db_config)

    csv_processor = CSVProcessor(
        db_manager=db_manager,
        csv_file_path="./users.csv",
        table_name="users",
        csv_has_header=True,
        strict_mode=False,
        use_zero_for_null_numerics=True,
        schema=EXAMPLE_SCHEMA,
    )
    csv_processor.process_csv()

    db_manager.close()

    logging.info("Done converting the CSV file to a Postgres table.")
