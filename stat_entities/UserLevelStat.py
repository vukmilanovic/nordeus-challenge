import json


class UserLevelStat:
    def __init__(
        self,
        user_id,
        name,
        country,
        number_of_logins,
        session_num,
        time_spent,
        date=None,
        inactive_days=None,
        last_login=None,
    ):
        self.user_id = user_id
        self.name = name
        self.date = date
        self.country = country
        self.number_of_logins = number_of_logins
        self.session_num = session_num
        self.time_spent = time_spent
        self.last_login = last_login
        self.inactive_days = inactive_days

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__)
