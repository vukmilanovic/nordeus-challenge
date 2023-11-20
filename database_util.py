from sql_queries import create_table_queries, drop_table_queries
from sqlalchemy import create_engine, exc


def connecti_to_db():
    engine = create_engine("postgresql://postgres:admin@localhost:5432/nordeus")
    conn = engine.raw_connection()
    cursor = conn.cursor()
    return conn, cursor


def close_connection(conn, cursor):
    cursor.close()
    conn.close()


def create_tables(conn, cursor):
    try:
        for query in create_table_queries:
            cursor.execute(query)
            conn.commit()
    except (Exception, exc.SQLAlchemyError) as error:
        print("Error while creating tables: %s" % error)
        conn.rollback()
        return
    print("Tables successfully created.")


def drop_tables(conn, cursor):
    try:
        for query in drop_table_queries:
            cursor.execute(query)
            conn.commit()
    except (Exception, exc.SQLAlchemyError) as error:
        print("Error while dropping tables: %s" % error)
        conn.rollback()
        return
    print("Tables successfully dropped.")
