import pandas as pd
from datetime import datetime

df_events = pd.read_json("./data/events.jsonl", lines=True)
df_exchange_rates = pd.read_json("./data/exchange_rates.jsonl", lines=True)
print(df_events.shape)

# creating temp column
df_events["user_id"] = df_events["event_data"].apply(lambda x: x["user_id"])

# droping duplicates
df_events = df_events.drop_duplicates(
    subset=[
        "event_type",
        "event_timestamp",
        "user_id",
    ],
    keep="first",
).reset_index(drop=True)

# deleting rows with wrong "device_os" value
valid_device_os = ["Web", "iOS", "Android"]
rows_to_keep = (df_events["event_type"] != "registration") | (
    df_events["event_data"].apply(lambda x: x.get("device_os"))
).isin(valid_device_os)
df_events = df_events[rows_to_keep]

# deleting rows with wrong transaction values
valid_transaction_amounts = [0.99, 1.99, 2.99, 4.99, 9.99]
valid_transaction_currency = ["EUR", "USD"]
rows_to_keep = (df_events["event_type"] != "transaction") | (
    (df_events["event_data"].apply(lambda x: x.get("transaction_amount"))).isin(
        valid_transaction_amounts
    )
    & (df_events["event_data"].apply(lambda x: x.get("transaction_currency"))).isin(
        valid_transaction_currency
    )
)
df_events = df_events[rows_to_keep]

# converting from UNIX timestamp to reliable date format
df_events["event_datetime"] = pd.to_datetime(df_events["event_timestamp"], unit="s")
df_events["date"] = df_events["event_datetime"].dt.strftime("%Y-%m-%d")

print(df_events.shape)

# check for multiple registration event of each user
registration_events_cond = df_events["event_type"] == "registration"
valid_registration_events = (
    df_events[registration_events_cond]
    .sort_values(by=["user_id", "date"])
    .groupby("user_id")
    .head(1)
)
df_events = df_events[
    (df_events["event_type"] != "registration")
    | (df_events.index.isin(valid_registration_events.index))
]

# eliminating invalid events that happened before the registration of each user
registration_dates = df_events[df_events["event_type"] == "registration"].set_index(
    "user_id"
)["date"]
reg_mask = df_events["date"] < df_events["user_id"].map(registration_dates)
df_events = df_events[~reg_mask]

print(df_events.shape)

df_events = df_events.sort_values(by=["user_id", "event_datetime"])

# eliminating events of same type in real time by user that appear twice or more in a row
df_events["prev_et"] = df_events.apply(lambda row: row.shift(1))["event_type"]
invalid_mask = (df_events["event_type"] != "transaction") & (
    df_events["event_type"] == df_events["prev_et"]
)
df_events = df_events[~invalid_mask]

# eliminating other invalid events (transactions, logouts)
# creating masks for invalid transactions and logouts
invalid_transaction_mask_prev = (df_events["event_type"] == "transaciton") & (
    (df_events["prev_et"] == "logout") | (df_events["prev_et"] == "registration")
)
invalid_logout_mask = (df_events["event_type"] == "logout") & (
    df_events["prev_et"] == "registration"
)
# combining masks
invalid_events_mask = invalid_transaction_mask_prev | invalid_logout_mask

# df filtering to discard invalid events
df_events = df_events[~invalid_events_mask]

print(df_events.shape)

df_events["country"] = None
df_events["name"] = None
registration_events_cond = df_events["event_type"] == "registration"

# appending "country" column to each row
df_events.loc[registration_events_cond, "country"] = df_events.loc[
    registration_events_cond, "event_data"
].apply(lambda x: x.get("country"))
df_events["country"] = df_events["user_id"].map(
    df_events.loc[registration_events_cond].set_index("user_id")["country"]
)

# appending "name" column to each row
df_events.loc[registration_events_cond, "name"] = df_events.loc[
    registration_events_cond, "event_data"
].apply(lambda x: x.get("name"))
df_events["name"] = df_events["user_id"].map(
    df_events.loc[registration_events_cond].set_index("user_id")["name"]
)

# discarding events whose "country" attribute is not correct
df_events = df_events[df_events["country"].str.len() == 2]

# print(df_events[df_events["event_type"] == "transaction"])
print(df_events.shape)
