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
from stat_entities.GameLevelStat import GameLevelStat
from stat_entities.UserLevelStat import UserLevelStat
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

    result = []
    if date is None:
        query = uls_user_query % (user_id)
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            uls = UserLevelStat(
                user_id=row[0],
                name=row[1],
                country=row[2],
                number_of_logins=row[3],
                session_num=row[4],
                time_spent=row[5],
                inactive_days=row[6],
            )
            result.append(uls.toJSON())
        close_connection(conn, cursor)
        return result
    query = uls_query % (user_id, date)
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        uls = UserLevelStat(
            user_id=row[0],
            name=row[1],
            country=row[2],
            date=row[3].isoformat(),
            number_of_logins=row[4],
            session_num=row[5],
            time_spent=row[6],
            last_login=row[7],
        )
        result.append(uls.toJSON())
    close_connection(conn, cursor)
    return result


@app.route("/api/game_stat", methods=["GET"])
def game_level_stat():
    args = request.args
    country = args.get("country")
    date = args.get("date")
    conn, cursor = connecti_to_db()

    result = []
    if None not in (country, date):
        query = gls_query % (date, country)
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            gls = GameLevelStat(
                country=row[0],
                date=row[1].isoformat(),
                number_of_logins=row[2],
                active_users=row[3],
                total_revenue_usd=row[4],
                paid_users=row[5],
                avg_session_num=row[6],
                avg_time_spent=row[9],
            )
            result.append(gls.toJSON())
        close_connection(conn, cursor)
        return result
    elif country is not None:
        query = gls_country_query % (country)
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            gls = GameLevelStat(
                country=row[0],
                number_of_logins=row[1],
                active_users=row[2],
                total_revenue_usd=row[3],
                paid_users=row[4],
                avg_session_num=row[5],
                avg_time_spent=row[6],
            )
            result.append(gls.toJSON())
        close_connection(conn, cursor)
        return result
    elif date is not None:
        query = gls_date_query % (date)
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            gls = GameLevelStat(
                date=row[0].isoformat(),
                number_of_logins=row[1],
                active_users=row[2],
                total_revenue_usd=row[3],
                paid_users=row[4],
                avg_session_num=row[5],
                avg_time_spent=row[6],
            )
            result.append(gls.toJSON())
        close_connection(conn, cursor)
        return result

    cursor.execute(gls_none_query)
    rows = cursor.fetchall()
    for row in rows:
        gls = GameLevelStat(
            number_of_logins=row[0],
            active_users=row[1],
            total_revenue_usd=row[2],
            paid_users=row[3],
            avg_session_num=row[4],
            avg_time_spent=row[5],
        )
        result.append(gls.toJSON())
    close_connection(conn, cursor)
    return result


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
