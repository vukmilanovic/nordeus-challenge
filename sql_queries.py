# dimension tables
user_table_drop = "DROP TABLE IF EXISTS consumer"
country_table_drop = "DROP TABLE IF EXISTS country"
time_table_drop = "DROP TABLE IF EXISTS time"

user_table_create = """CREATE TABLE IF NOT EXISTS consumer ( 
    user_id varchar PRIMARY KEY NOT NULL, 
    name varchar, 
    country varchar(2) 
);"""

country_table_create = """CREATE TABLE IF NOT EXISTS country ( 
    name varchar(2) PRIMARY KEY NOT NULL 
);"""

time_table_create = """CREATE TABLE IF NOT EXISTS time ( 
    day date PRIMARY KEY NOT NULL 
);"""

user_table_insert = """ 
    INSERT INTO consumer (
        user_id, 
        name, 
        country) VALUES (%s, %s, %s)
    """

country_table_insert = """
    INSERT INTO country (
        name) VALUES (%s)
    """

time_table_insert = """
    INSERT INTO time (
        day) VALUES (%s) 
    """

# fact tables
user_level_stat_table_drop = "DROP TABLE IF EXISTS user_level_stat"
game_level_stat_table_drop = "DROP TABLE IF EXISTS game_level_stat"

user_level_stat_table_create = """CREATE TABLE IF NOT EXISTS user_level_stat ( 
    user_id varchar NOT NULL references consumer(user_id), 
    date date NOT NULL references time(day), 
    number_of_logins int NOT NULL, 
    last_login int NOT NULL, 
    session_num int NOT NULL, 
    time_spent int NOT NULL, 
    inactive_days float, 
    constraint user_level_stat_pk PRIMARY KEY (user_id, date) 
);"""

game_level_stat_table_create = """CREATE TABLE IF NOT EXISTS game_level_stat ( 
    country varchar(2) NOT NULL references country(name), 
    date date NOT NULL references time(day), 
    number_of_logins int NOT NULL, 
    active_users int NOT NULL, 
    total_revenue_usd float NOT NULL, 
    paid_users int NOT NULL, 
    avg_session_num float NOT NULL, 
    session_sum int NOT NULL, 
    session_users_count int NOT NULL, 
    avg_time_spent float NOT NULL, 
    time_spent_sum int NOT NULL, 
    time_spent_users_count int NOT NULL, 
    constraint game_level_stat_pk PRIMARY KEY (country, date)
);"""

user_level_stat_table_insert = """
    INSERT INTO user_level_stat (
        user_id,
        date,
        number_of_logins,
        last_login,
        session_num,
        time_spent,
        inactive_days) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

game_level_stat_table_insert = """
    INSERT INTO game_level_stat (
        country,
        date,
        number_of_logins,
        active_users,
        total_revenue_usd,
        paid_users,
        avg_session_num,
        session_sum,
        session_users_count,
        avg_time_spent,
        time_spent_sum,
        time_spent_users_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

# SELECT QUERIES
# user_level_stat queries

uls_user_query = """
    SELECT uls.user_id, u.name, u.country,
        uls.number_of_logins,
        uls.session_num,
        uls.time_spent,
        uls.inactive_days
    FROM (
        SELECT user_id, inactive_days,
            sum(number_of_logins) as number_of_logins, 
            sum(session_num) as session_num, 
            sum(time_spent) as time_spent
        FROM user_level_stat
        GROUP BY user_id, inactive_days
        HAVING user_id = '%s'
    ) uls LEFT OUTER JOIN consumer u
    ON uls.user_id = u.user_id;
"""

uls_query = """
    SELECT uls.user_id, u.name, u.country, uls.date,
        uls.number_of_logins, uls.session_num,
        uls.time_spent, uls.last_login
    FROM user_level_stat uls LEFT OUTER JOIN consumer u
    ON uls.user_id = u.user_id
    WHERE uls.user_id = '%s' AND uls.date = '%s';
"""

# game_level_stat queries

gls_none_query = """
    SELECT sum(number_of_logins) as number_of_logins,
        sum(active_users) as active_users, 
        sum(total_revenue_usd) as total_revenue_usd, 
        sum(paid_users) as paid_users,
        sum(session_sum)/sum(session_users_count) as avg_session_num, 
        sum(time_spent_sum)/sum(time_spent_users_count) as avg_time_spent 
    FROM game_level_stat gls;
"""
gls_country_query = """
    SELECT gls.country, sum(number_of_logins) as number_of_logins, 
        sum(active_users) as active_users, 
        sum(total_revenue_usd) as total_revenue_usd, 
        sum(paid_users) as paid_users, 
        sum(session_sum)/sum(session_users_count) as avg_session_num, 
        sum(time_spent_sum)/sum(time_spent_users_count) as avg_time_spent
    FROM game_level_stat gls
    GROUP BY gls.country
    HAVING gls.country = '%s';
"""
gls_date_query = """
    SELECT gls.date, sum(number_of_logins) as number_of_logins, 
        sum(active_users) as active_users, 
        sum(total_revenue_usd) as total_revenue_usd, 
        sum(paid_users) as paid_users,
        sum(session_sum)/sum(session_users_count) as avg_session_num, 
        sum(time_spent_sum)/sum(time_spent_users_count) as avg_time_spent
    FROM game_level_stat gls
    GROUP BY gls.date
    HAVING gls.date = '%s';
"""
gls_query = """
    SELECT *
    FROM game_level_stat gls
    WHERE gls.date = '%s' AND gls.country = '%s';
"""

# DDL query lists

create_table_queries = [
    user_table_create,
    time_table_create,
    country_table_create,
    user_level_stat_table_create,
    game_level_stat_table_create,
]
drop_table_queries = [
    game_level_stat_table_drop,
    user_level_stat_table_drop,
    country_table_drop,
    time_table_drop,
    user_table_drop,
]
