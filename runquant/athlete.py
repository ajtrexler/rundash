import yaml
from datetime import datetime

'''
TODO:
 - handle resting_hr as list/array to account for deviations
 - handle other methods for HR computation
'''


class Athlete:
    def __init__(self, user_id: int, config: str = './user.yml'):
        self.config_path = config
        with open(self.config_path, 'r') as f:
            self.config = yaml.load(f, yaml.SafeLoader)

        self.birthday = datetime.strptime(self.config['birthday'], '%Y-%m-%d').date()
        self.resting_hr = self.config['resting_hr']
        self.gender = self.config['gender']

    def age(self, when: datetime = None):
        if when is None:
            when = datetime.today()
        when = when.date()
        return (when - self.birthday).days // 365

    def max_heart_rate(self, when: datetime = None):
        return 220 - self.age(when)
