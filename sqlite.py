from typing import Iterable

import sqlite3


def execute_statement(queries: Iterable[str], database_file_path: str) -> None:
    connection = sqlite3.connect(database_file_path)
    cursor = connection.cursor()
    for query in queries:
        cursor.execute(query)
    connection.commit()
    connection.close()
