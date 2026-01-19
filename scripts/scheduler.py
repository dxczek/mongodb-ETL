"""
Daily ETL scheduler - runs the ETL pipeline at a scheduled time every day.
"""

import schedule
import time
import os
import subprocess
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SCHEDULE_TIME = os.getenv('SCHEDULE_TIME', '02:00')

def run_etl():
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'\n⏰ {now} - Running ETL pipeline...')
    subprocess.run(['python', 'scripts/etl_pipeline.py'])

schedule.every().day.at(SCHEDULE_TIME).do(run_etl)

print(f'📅 Scheduler started.')
print(f'   ETL will run daily at {SCHEDULE_TIME}')
print(f'   (Press Ctrl+C to stop)\n')

try:
    while True:
        schedule.run_pending()
        time.sleep(60)
except KeyboardInterrupt:
    print('\n🛑 Scheduler stopped')
