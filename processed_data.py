import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from game_level_stat import game_level_df, user_level_df, df_events

# dimension tables data
user_df = (
    df_events[["user_id", "name", "country"]]
    .sort_values(by="user_id")
    .drop_duplicates(subset=["user_id", "name", "country"], keep="first")
)
country_list = df_events["country"].sort_values().unique()
country_df = pd.DataFrame({"name": country_list})
date_list = df_events["date"].sort_values().unique()
date_df = pd.DataFrame({"day": date_list})

# fact tables data (user_level_df & game_level_df)
user_level_df = user_level_df[
    [
        "user_id",
        "date",
        "number_of_logins",
        "last_login",
        "session_num",
        "time_spent",
        "inactive_days",
    ]
]

# data insert
engine = create_engine("postgresql://postgres:admin@localhost:5432/nordeus")
conn = engine.connect()

user_df.to_sql("consumer", conn, if_exists="append", index=False)
country_df.to_sql("country", conn, if_exists="append", index=False)
date_df.to_sql("time", conn, if_exists="append", index=False)
user_level_df.to_sql("user_level_stat", conn, if_exists="append", index=False)
game_level_df.to_sql("game_level_stat", conn, if_exists="append", index=False)
