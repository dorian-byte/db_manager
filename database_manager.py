import logging
import psycopg2

from psycopg2 import errors


class DatabaseManager:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = self.create_connection()

    def create_connection(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            connection = psycopg2.connect(**self.db_config)
            return connection
        except Exception as error:
            logging.error(
                f"Error: Could not connect to the PostgreSQL database. \n{error}"
            )
            return None

    def ensure_table_exists(self, schema, table_name):
        # checks if the table exists and creates it if it doesn't
        if not self.table_exists(table_name):
            self.create_table(schema, table_name)

    def table_exists(self, table_name):
        # checks if the table exists and returns True or False
        with self.connection as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')"
                )
                return cursor.fetchone()[0]

    def create_table(self, schema, table_name=None):
        properties = schema["properties"]

        columns = []
        for column_name, details in properties.items():
            sql_part = f"{column_name} {self._get_sql_type(details)}"
            if "format" in details and details["format"] == "email":
                sql_part += " UNIQUE"
            if column_name in schema["required"]:
                sql_part += " NOT NULL"
            columns.append(sql_part)

        create_table_command = f"""
        CREATE TABLE {table_name} (
            {', '.join(columns)}
        );
        """

        with self.connection as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_command)
                conn.commit()

    def drop_table(self, table_name):
        """
        Drop a table from the database. Dangerous!
        """
        with self.connection as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE {table_name}")
                conn.commit()

    def _get_sql_type(self, detail):
        type_mapping = {
            "integer": "SERIAL",
            "string": {
                "default": "VARCHAR(255)",
                "email": "VARCHAR(255)",
                "date-time": "TIMESTAMP WITH TIME ZONE",
            },
            "boolean": "BOOLEAN",
        }

        datatype = detail["type"]
        if datatype == "string":
            format_ = detail.get("format")
            return type_mapping["string"].get(
                format_, type_mapping["string"]["default"]
            )

        return type_mapping[datatype]

    def insert_data(self, table_name, data, schema):
        self.ensure_table_exists(schema, table_name)
        with self.connection as conn:
            with conn.cursor() as cursor:
                try:
                    placeholders = ", ".join(["%s"] * len(data[0]))
                    columns = ", ".join(schema["properties"].keys())
                    cursor.executemany(
                        f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
                        data,
                    )
                    conn.commit()
                except (
                    errors.UndefinedTable
                ):  # this is the specific error for a nonexistent table in Postgres
                    self.create_table(schema, table_name)
                    self.insert_data(
                        table_name, data, schema
                    )  # retry insert after table creation
                except Exception as e:
                    print(f"Failed to insert data: {e}")

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
