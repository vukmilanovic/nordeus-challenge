import json


class GameLevelStat:
    def __init__(
        self,
        number_of_logins,
        active_users,
        total_revenue_usd,
        paid_users,
        avg_session_num,
        avg_time_spent,
        country=None,
        date=None,
    ):
        self.country = country
        self.date = date
        self.number_of_logins = number_of_logins
        self.active_users = active_users
        self.total_revenue_usd = total_revenue_usd
        self.paid_users = paid_users
        self.avg_session_num = avg_session_num
        self.avg_time_spent = avg_time_spent

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__)
