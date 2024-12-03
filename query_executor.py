import mysql.connector
from prettytable import PrettyTable


class QueryExecutor:
    def __init__(self):
        self.host = "localhost"
        self.user = "root"
        self.password = "welcome1"
        self.database = "finalstore"

    def execute_query(self, query, description):
        print(f"\n{description}")
        conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        cursor = conn.cursor()

        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            # Display results in a table
            table = PrettyTable()
            table.field_names = column_names
            for row in rows:
                table.add_row(row)

            print(table)
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            cursor.close()
            conn.close()
