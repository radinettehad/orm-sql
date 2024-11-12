import sqlite3
from typing import List, Tuple, Any, Dict


class SqliteConnection:
    _conn: sqlite3.Connection = None

    @classmethod
    def get_connection(cls) -> sqlite3.Connection:
        """Return a SQLite3 connection."""
        if cls._conn is None:
            cls._conn = sqlite3.connect('db.sqlite3')
            cls._conn.row_factory = sqlite3.Row  # Allow access to columns by name
            cls._conn.autocommit = True
        return cls._conn

    @classmethod
    def get_cursor(cls) -> sqlite3.Cursor:
        """Return a SQLite3 cursor."""
        return cls.get_connection().cursor()

    @classmethod
    def get_all(cls, table_name: str) -> List[Dict[str, Any]]:
        """Return all rows from the specified table."""
        query = f"SELECT * FROM {table_name}"
        db_cursor = cls.get_cursor()
        db_cursor.execute(query)
        return [dict(row) for row in db_cursor.fetchall()]

    @classmethod
    def get_by_id(cls, table_name: str, item_id: int) -> Dict[str, Any]:
        """Return a single row that matches the provided ID."""
        query = f"SELECT * FROM {table_name} WHERE id = ?"
        db_cursor = cls.get_cursor()
        db_cursor.execute(query, (item_id,))
        return dict(db_cursor.fetchone()) if db_cursor.fetchone() else None

    @classmethod
    def get_by_expression(cls, table_name: str, **kwargs) -> List[Dict[str, Any]]:
        """Return rows matching multiple conditions (with OR)."""
        conditions = " OR ".join(f"{key} = ?" for key in kwargs.keys())
        condition_values = tuple(kwargs.values())
        query = f"SELECT * FROM {table_name} WHERE {conditions}"
        cursor = cls.get_cursor()
        cursor.execute(query, condition_values)
        return [dict(row) for row in cursor.fetchall()]

    @classmethod
    def update_table(cls, table_name: str, item_id: int, **kwargs) -> bool:
        """Update a row in the specified table by ID."""
        if not kwargs:
            return False

        set_clause = ", ".join(f"{key} = ?" for key in kwargs.keys())
        values = tuple(kwargs.values())
        query = f"UPDATE {table_name} SET {set_clause} WHERE id = ?"

        try:
            cursor = cls.get_cursor()
            cursor.execute(query, values + (item_id,))
            return cursor.rowcount > 0  # Return True if at least one row was updated
        except Exception as e:
            print(f"Error updating table: {e}")  # Can optionally log or handle the exception differently
            return False

    @classmethod
    def insert_into_table(cls, table_name: str, **kwargs) -> bool:
        """Insert a new row into the specified table."""
        if not kwargs:
            return False

        columns = ', '.join(kwargs.keys())
        placeholders = ', '.join('?' for _ in kwargs)
        values = tuple(kwargs.values())
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        try:
            cursor = cls.get_cursor()
            cursor.execute(query, values)
            return cursor.lastrowid > 0  # Return True if a new row was inserted
        except Exception as e:
            print(f"Error inserting into table: {e}")
            return False

    @classmethod
    def delete_from_table(cls, table_name: str, item_id: int) -> bool:
        """Delete a row from the specified table by ID."""
        query = f"DELETE FROM {table_name} WHERE id = ?"
        try:
            cursor = cls.get_cursor()
            cursor.execute(query, (item_id,))
            return cursor.rowcount > 0  # Return True if at least one row was deleted
        except Exception as e:
            print(f"Error deleting from table: {e}")
            return False


if __name__ == '__main__':
    sqlite_instance = SqliteConnection()

    # Example usages
    author_details = sqlite_instance.get_by_id(table_name='Humans_author', item_id=6)
    print(author_details)

    is_updated = sqlite_instance.update_table(table_name='Humans_author', item_id=6,
                                              author_name_en='George Orwell', author_name_fa='جورج اورول')
    print("Update successful:", is_updated)

    author_details = sqlite_instance.get_by_id(table_name='Humans_author', item_id=6)
    print(author_details)