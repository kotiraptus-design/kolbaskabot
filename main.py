"""
Telegram бот для рассылки дежурных БЕЗ pandas
"""

import os
import asyncio
import logging
import sqlite3
import tempfile
from datetime import datetime, date
from typing import List, Dict, Optional
import io
import csv

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import ContentType
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TIMEZONE = pytz.timezone("Europe/Moscow")

# Конфигурация
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = [int(x.strip()) for x in os.getenv('ADMIN_IDS', '').split(',') if x.strip().isdigit()]
DEFAULT_SEND_TIME = os.getenv('DEFAULT_SEND_TIME', '09:00')
DATA_DIR = '/tmp/data'
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, 'duty_bot.db')

if not BOT_TOKEN:
    raise RuntimeError('Установите BOT_TOKEN')

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# БАЗА ДАННЫХ
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS duties (
        id INTEGER PRIMARY KEY,
        duty_date TEXT NOT NULL,
        name TEXT NOT NULL
    )
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    ''')
    cur.execute('''
    CREATE TABLE IF NOT EXISTS recipients (
        chat_id INTEGER PRIMARY KEY
    )
    ''')
    conn.commit()
    conn.close()

init_db()

# УТИЛИТЫ
def set_config(key: str, value: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('REPLACE INTO config (key, value) VALUES (?, ?)', (key, value))
    conn.commit()
    conn.close()

def get_config(key: str) -> Optional[str]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT value FROM config WHERE key=?', (key,))
    r = cur.fetchone()
    conn.close()
    return r[0] if r else None

def add_recipient(chat_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('INSERT OR IGNORE INTO recipients (chat_id) VALUES (?)', (chat_id,))
    conn.commit()
    conn.close()

def list_recipients() -> List[int]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT chat_id FROM recipients')
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]

def insert_duties(records: List[Dict]):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executemany('INSERT INTO duties (duty_date, name) VALUES (?, ?)',
                    [(r['date'], r['name']) for r in records])
    conn.commit()
    conn.close()

def get_duties_for_date(d: date) -> List[str]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT name FROM duties WHERE duty_date=?', (d.isoformat(),))
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]

# ПРОСТОЙ ПАРСЕР CSV
def parse_text_file(content: bytes, filename: str) -> List[Dict]:
    """Парсит текстовые файлы (CSV, TXT)"""
    records = []
    text = content.decode('utf-8', errors='ignore')
    
    # Определяем разделитель
    lines = text.split('\n')
    if not lines:
        return records
    
    # Ищем заголовки
    first_line = lines[0].lower()
    if 'дата' in first_line and ('имя' in first_line or 'фио' in first_line):
        # CSV с заголовками
        delimiter = ',' if ',' in first_line else (';' if ';' in first_line else '\t')
        
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        for row in reader:
            # Ищем колонки с датой и именем
            date_col = None
            name_col = None
            
            for col in row:
                col_lower = col.lower()
                if 'дата' in col_lower or 'date' in col_lower:
                    date_col = col
                elif 'имя' in col_lower or 'фио' in col_lower or 'name' in col_lower:
                    name_col = col
            
            if date_col and name_col and row[date_col] and row[name_col]:
                date_str = row[date_col].strip()
                name = row[name_col].strip()
                
                # Парсим дату
                try:
                    if '-' in date_str:
                        d = datetime.strptime(date_str, '%Y-%m-%d').date()
                    elif '.' in date_str:
                        d = datetime.strptime(date_str, '%d.%m.%Y').date()
                    elif '/' in date_str:
                        d = datetime.strptime(date_str, '%d/%m/%Y').date()
                    elif date_str.isdigit():
                        today = datetime.now(TIMEZONE).date()
                        d = date(today.year, today.month, int(date_str))
                    else:
                        continue
                    
                    records.append({'date': d.isoformat(), 'name': name})
                except:
                    continue
    
    return records

# РАССЫЛКА
async def send_today_message():
    today = datetime.now(TIMEZONE).date()
    names = get_duties_for_date(today)
    
    if not names:
        text = f'На {today.isoformat()} дежурных не найдено.'
    else:
        text = f'Дежурные на {today.isoformat()}:\n' + '\n'.join(f'- {n}' for n in names)
    
    recipients = list_recipients()
    if not recipients:
        recipients = ADMIN_IDS
    
    for chat_id in recipients:
        try:
            await bot.send_message(chat_id, text)
        except Exception as e:
            logger.error(f"Не удалось отправить в {chat_id}: {e}")

# ПЛАНИРОВЩИК
scheduler = AsyncIOScheduler(timezone=TIMEZONE)

def schedule_daily(send_time: str):
    scheduler.remove_all_jobs()
    hh, mm = [int(x) for x in send_time.split(':')]
    trigger = CronTrigger(hour=hh, minute=mm)
    scheduler.add_job(lambda: asyncio.create_task(send_today_message()), trigger)
    set_config('send_time', send_time)

# КОМАНДЫ
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        await message.reply("👑 Бот для рассылки дежурных\n/send_today - отправить\nОтправьте CSV файл с колонками: дата, имя")
    else:
        await message.reply("помидор")

@dp.message(Command("send_today"))
async def cmd_send_today(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Только админы")
        return
    
    await send_today_message()
    await message.reply('✅ Отправлено')

@dp.message(Command("set_time"))
async def cmd_set_time(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Только админы")
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply('Использование: /set_time HH:MM')
        return
    
    t = parts[1].strip()
    try:
        hh, mm = map(int, t.split(':'))
        assert 0 <= hh < 24 and 0 <= mm < 60
        schedule_daily(t)
        await message.reply(f'✅ Время установлено: {t}')
    except:
        await message.reply('❌ Неверный формат')

# ЗАГРУЗКА ФАЙЛОВ
@dp.message(F.document)
async def handle_docs(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply('❌ Только админы')
        return
    
    doc = message.document
    fname = doc.file_name or 'uploaded.csv'
    
    # Принимаем текстовые файлы
    if not any(fname.lower().endswith(ext) for ext in ('.csv', '.txt', '.xls', '.xlsx')):
        await message.reply('❌ Загрузите CSV или текстовый файл')
        return
    
    # Скачиваем файл
    file_data = io.BytesIO()
    await bot.download(doc, destination=file_data)
    file_data.seek(0)
    
    await message.reply('📥 Файл получен, парсинг...')
    
    try:
        records = parse_text_file(file_data.read(), fname)
        if not records:
            await message.reply('❌ Не удалось распарсить. Нужны колонки: дата, имя')
            return
        
        # Очищаем старые данные
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('DELETE FROM duties')
        conn.commit()
        conn.close()
        
        # Добавляем новые
        insert_duties(records)
        await message.reply(f'✅ Импортировано: {len(records)} записей')
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await message.reply('❌ Ошибка обработки файла')

# ЗАПУСК
async def on_startup():
    send_time = get_config('send_time') or DEFAULT_SEND_TIME
    schedule_daily(send_time)
    scheduler.start()
    logger.info('Бот запущен')

async def main():
    await on_startup()
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
