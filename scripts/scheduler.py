import schedule
import time
import os
import subprocess
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SCHEDULE_TIME = os.getenv('SCHEDULE_TIME', '02:00')

def run_etl():
    """Uruchom ETL pipeline"""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'\n⏰ {now} - Uruchamianie ETL...')
    subprocess.run(['python', 'scripts/etl_pipeline.py'])

# Zaplanuj ETL
schedule.every().day.at(SCHEDULE_TIME).do(run_etl)

print(f'📅 Scheduler uruchomiony.')
print(f'   ETL będzie uruchamiany codziennie o {SCHEDULE_TIME}')
print(f'   (Wciśnij Ctrl+C aby zatrzymać)\n')

# Główna pętla
try:
    while True:
        schedule.run_pending()
        time.sleep(60)
except KeyboardInterrupt:
    print('\n🛑 Scheduler wyłączony')