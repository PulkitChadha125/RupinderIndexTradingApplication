from datetime import datetime, timedelta
current_datetime = datetime.now()
last_trade_time = current_datetime.time()
new_time_seconds = last_trade_time.hour * 3600 + last_trade_time.minute * 60 + 20 * 60
new_time_minutes = (new_time_seconds // 60 + 4) // 5 * 5
new_time_seconds = new_time_minutes * 60
new_time = datetime.utcfromtimestamp(new_time_seconds).time()
current_time = datetime.now().time()
print("current_time",current_time)


print(new_time)