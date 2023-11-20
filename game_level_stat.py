import pandas as pd
from datetime import datetime
from cleaning_data import df_events, df_exchange_rates
from user_level_stat import user_level_df

# GAME_LEVEL_STAT

# creating game_level_stat df with number_of_logins column
login_events_cond = df_events["event_type"] == "login"

game_level_df = (
    df_events[login_events_cond]
    .groupby(["country", "date"])
    .size()
    .reset_index(name="number_of_logins")
)

# appending active_users column
game_level_df["active_users"] = (
    df_events[login_events_cond]
    .drop_duplicates(subset=["country", "date", "event_type", "user_id"], keep="first")
    .groupby(["country", "date"])
    .size()
    .reset_index(name="active_users")
)["active_users"]

# appending transaction attributes to original df
transaction_events_cond = df_events["event_type"] == "transaction"

df_events["transaction_amount"] = df_events[transaction_events_cond][
    "event_data"
].apply(lambda x: x.get("transaction_amount"))
df_events["currency"] = df_events[transaction_events_cond]["event_data"].apply(
    lambda x: x.get("transaction_currency")
)

# converting each transaction to USD
df_events = pd.merge(
    df_events,
    df_exchange_rates,
    how="left",
    on="currency",
)

# creating mask to fetch transactions that are not proceeded with USD currency
mask = df_events["currency"] != "USD"
# converting each transaction amount to dollars
df_events.loc[mask, "currency"] = "USD"
df_events.loc[mask, "transaction_amount"] *= df_events["rate_to_usd"]

# finding total_revenue_usd
transaction_events_cond = df_events["event_type"] == "transaction"
temp_total_revenue_df = (
    df_events[transaction_events_cond]
    .groupby(["country", "date"])["transaction_amount"]
    .sum()
    .reset_index(name="total_revenue_usd")
)

# merging temporary data frame with original (adding total_revenue_usd column)
game_level_df = pd.merge(
    game_level_df, temp_total_revenue_df, on=("country", "date"), how="left"
)
game_level_df["total_revenue_usd"] = (
    game_level_df["total_revenue_usd"].fillna(0).astype(float)
)

registration_events_cond = df_events["event_type"] == "registration"
df_events["marketing_campaign"] = df_events[registration_events_cond][
    "event_data"
].apply(lambda x: x.get("marketing_campaign"))

# checking marketing campaign values
# print(df_events[registration_events_cond]["marketing_campaign"].unique())

# appending column paid_users
invalid_paid_user = [None, ""]
paid_users_cond = registration_events_cond & (
    ~df_events["marketing_campaign"].isin(invalid_paid_user)
)
temp_paid_users_df = (
    df_events[paid_users_cond]
    .groupby(["country", "date"])
    .size()
    .reset_index(name="paid_users")
)
game_level_df = pd.merge(
    game_level_df, temp_paid_users_df, on=["country", "date"], how="left"
)
game_level_df["paid_users"] = game_level_df["paid_users"].fillna(0).astype(int)

# appending "avg_num_session" column
# discarding users with no sessions
no_session_mask = user_level_df["session_num"] == 0
session_users_df = user_level_df[~no_session_mask]
# calculating average session number
temp_session_df = session_users_df.groupby(["country", "date"])["session_num"].agg(
    avg_session_num="mean", session_sum="sum", session_users_count="count"
)
game_level_df = pd.merge(
    game_level_df, temp_session_df, how="left", on=["country", "date"]
)
game_level_df["avg_session_num"] = (
    game_level_df["avg_session_num"].fillna(0).astype(float).round(3)
)

# appending "avg_time_spent" column
spent_time_mask = user_level_df["time_spent"] != 0
time_spent_df = user_level_df[spent_time_mask]

# calculating average time spent
temp_time_spent_df = time_spent_df.groupby(["country", "date"])["time_spent"].agg(
    avg_time_spent="mean", time_spent_sum="sum", time_spent_users_count="count"
)
game_level_df = pd.merge(
    game_level_df, temp_time_spent_df, how="left", on=["country", "date"]
)
game_level_df["avg_time_spent"] = (
    game_level_df["avg_time_spent"].fillna(0).astype(float).round(3)
)
