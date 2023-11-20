from flask import Flask, request, jsonify
from sql_queries import (
    uls_query,
    uls_user_query,
    gls_country_query,
    gls_date_query,
    gls_none_query,
    gls_query,
)
import json
from sqlalchemy import create_engine
from flask_swagger_ui import get_swaggerui_blueprint
from database_util import connecti_to_db, close_connection, create_tables, drop_tables

app = Flask(__name__)

# Configure Swagger UI
SWAGGER_URL = "/swagger"
API_URL = "/swagger.json"
swagger_ui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL, API_URL, config={"app_name": "Nordeus Flask API"}
)

app.register_blueprint(swagger_ui_blueprint, url_prefix=SWAGGER_URL)


@app.route("/swagger.json")
def swagger():
    with open("swagger.json") as f:
        return jsonify(json.load(f))


@app.route("/api/user_stat", methods=["GET"])
def user_level_stat():
    args = request.args
    user_id = args.get("user_id")
    date = args.get("date")
    conn, cursor = connecti_to_db()

    result = None
    if date is None:
        query = uls_user_query % (user_id)
        cursor.execute(query)
        result = cursor.fetchall()
        close_connection(conn, cursor)
        print(result)
        return result
    query = uls_query % (user_id, date)
    cursor.execute(uls_query, (user_id, date))
    result = cursor.fetchall()
    close_connection(conn, cursor)
    return result


@app.route("/api/game_stat", methods=["GET"])
def game_level_stat():
    args = request.args
    country = args.get("country")
    date = args.get("date")
    conn, cursor = connecti_to_db()

    result = None
    if None not in (country, date):
        query = gls_query % (date, country)
        cursor.execute(query)
        result = cursor.fetchall()
        close_connection(conn, cursor)
        return result
    elif country is not None:
        query = gls_country_query % (country)
        cursor.execute(query)
        result = cursor.fetchall()
        close_connection(conn, cursor)
        return result
    elif date is not None:
        query = gls_date_query % (date)
        cursor.execute(query)
        result = cursor.fetchall()
        close_connection(conn, cursor)
        return result

    cursor.execute(gls_none_query)
    result = cursor.fetchall()
    close_connection(conn, cursor)
    return result


def connecti_to_db():
    engine = create_engine("postgresql://postgres:admin@localhost:5432/nordeus")
    conn = engine.raw_connection()
    cursor = conn.cursor()
    return conn, cursor


def close_connection(conn, cursor):
    cursor.close()
    conn.close()


def init():
    conn, cursor = connecti_to_db()

    drop_tables(conn, cursor)
    create_tables(conn, cursor)

    close_connection(conn, cursor)


if __name__ == "__main__":
    print(
        (
            "Loading Flask starting server..."
            "Please wait until server has fully started."
        )
    )
    init()
    app.run(host="0.0.0.0", port=105)
