import datetime

def get_generated_datetimes(start, end, step=60):
   print(start)
   print(end)
   generated_datetimes = []
   cur = start
   while cur < end:
      cur += datetime.timedelta(minutes=step)
      generated_datetimes.append(cur.isoformat())
   return generated_datetimes

print(get_generated_datetimes(
   datetime.datetime(2022, 12, 28, 20, 55, 59, 00000),
   datetime.datetime(2022, 12, 28, 23, 55, 59, 00000),
   60
))