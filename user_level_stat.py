import pandas as pd
from datetime import datetime
from cleaning_data import df_events

# USER_LEVEL_STAT

# appending "number_of_logins" column
user_level_df = (
    df_events.groupby(["user_id", "name", "country", "date"])
    .agg(number_of_logins=("event_type", lambda x: (x == "login").sum()))
    .reset_index()
)

# appending "last_login" column
df_events["date"] = pd.to_datetime(df_events["date"])
user_level_df["date"] = pd.to_datetime(user_level_df["date"])

df_events.sort_values(by=["user_id", "date"], inplace=True)
login_events_cond = df_events["event_type"] == "login"
df_events["last_login"] = (
    df_events[login_events_cond].groupby("user_id")["date"].diff().dt.days
)
df_events["last_login"] = (
    df_events.loc[login_events_cond, "last_login"].fillna(0).astype(int)
)

# eliminating duplicate events (multiple logins in a single day)
df_temp = df_events[df_events["last_login"] != 0].copy()

# merging results by user_id and date columns
user_level_df = pd.merge(
    user_level_df,
    df_temp.loc[login_events_cond, ["user_id", "date", "last_login"]],
    how="left",
    on=["user_id", "date"],
)
user_level_df["last_login"] = user_level_df["last_login"].fillna(0).astype(int)

# session number per user each day
session_df = (
    df_events[df_events["event_type"] == "logout"]
    .groupby(["user_id", "date"])
    .size()
    .reset_index(name="session_num")
)
user_level_df = pd.merge(user_level_df, session_df, how="left", on=["user_id", "date"])
user_level_df["session_num"] = user_level_df["session_num"].fillna(0).astype(int)


# time spent in game (s) per user each day
df_events["time_spent"] = (
    df_events[
        (df_events["event_type"] == "login") | (df_events["event_type"] == "logout")
    ]
    .sort_values(by="event_timestamp")
    .groupby("user_id")["event_timestamp"]
    .diff()
)
time_spent_df = (
    df_events[df_events["event_type"] == "logout"]
    .groupby(["user_id", "date"])["time_spent"]
    .sum()
    .reset_index(name="time_spent")
)
user_level_df = pd.merge(
    user_level_df, time_spent_df, on=["user_id", "date"], how="left"
)
user_level_df["time_spent"] = user_level_df["time_spent"].fillna(0).astype(int)

# filter last logouts of each user
logout_df = (
    df_events[df_events["event_type"] == "logout"]
    .sort_values(by="event_timestamp")
    .groupby(by="user_id")
    .tail(1)[["user_id", "date"]]
)
logout_df.rename(columns={"date": "last_login_date"}, inplace=True)
# search max date in dataset
max_dataset_date = df_events["date"].unique().max()
# calculating inactive days
logout_df["inactive_days"] = (max_dataset_date - logout_df["last_login_date"]).dt.days
logout_df = logout_df.drop("last_login_date", axis=1)
user_level_df = pd.merge(user_level_df, logout_df, how="left", on="user_id")
