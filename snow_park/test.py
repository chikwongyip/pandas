# coding:utf-8
from datetime import datetime, timedelta

today = datetime.today()
f_day = today + timedelta(days=10)
print('today', today, f_day.date())
