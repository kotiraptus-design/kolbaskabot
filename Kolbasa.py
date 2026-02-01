"""
Telegram бот для ежедневной отправки дежурных из загружаемого Excel-списка.
Только администраторы имеют доступ к функциям управления.
Не-админам доступна только команда /start.
"""

import os
import asyncio
import logging
import sqlite3
from datetime import datetime, date
from typing import List, Dict, Optional

import pandas as pd
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import ContentType
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

# Настройка логов
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TIMEZONE = pytz.timezone("Europe/Moscow")

# Загрузка конфигурации
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = [int(x.strip()) for x in os.getenv('ADMIN_IDS', '').split(',') if x.strip().isdigit()]
DEFAULT_SEND_TIME = os.getenv('DEFAULT_SEND_TIME', '09:00')
DATA_DIR = os.getenv('DATA_DIR', './data')
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, 'duty_bot.db')

if not BOT_TOKEN:
    raise RuntimeError('Установите BOT_TOKEN в .env')

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ========== ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ==========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS duties (
        id INTEGER PRIMARY KEY,
        duty_date TEXT NOT NULL, -- ISO YYYY-MM-DD
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

# ========== УТИЛИТЫ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ ==========
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

def remove_recipient(chat_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('DELETE FROM recipients WHERE chat_id=?', (chat_id,))
    conn.commit()
    conn.close()

def list_recipients() -> List[int]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT chat_id FROM recipients')
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]

def clear_duties():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('DELETE FROM duties')
    conn.commit()
    conn.close()

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

def get_duties_for_month(year: int, month: int) -> Dict[str, List[str]]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    start = date(year, month, 1).isoformat()
    if month == 12:
        end = date(year + 1, 1, 1).isoformat()
    else:
        end = date(year, month + 1, 1).isoformat()
    cur.execute('''
        SELECT duty_date, name FROM duties
        WHERE duty_date >= ? AND duty_date < ?
        ORDER BY duty_date
    ''', (start, end))
    rows = cur.fetchall()
    conn.close()
    result: Dict[str, List[str]] = {}
    for d, name in rows:
        result.setdefault(d, []).append(name)
    return result

# ========== ПАРСЕР EXCEL ==========
DATE_HEADERS = ['дата', 'date', 'day', 'день']
NAME_HEADERS = ['имя', 'фио', 'name', 'дежурный', 'дежурные', 'person', 'employee']

def try_parse_date(x) -> Optional[date]:
    if pd.isna(x):
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.date()
    s = str(x).strip()
    for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d.%m.%y', '%d/%m/%Y', '%d %m %Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    if s.isdigit():
        val = int(s)
        today = datetime.now(TIMEZONE).date()
        try:
            return date(today.year, today.month, val)
        except Exception:
            return None
    return None

def parse_excel(path: str) -> List[Dict]:
    logger.info('Parsing excel: %s', path)
    xls = pd.read_excel(path, sheet_name=None, engine='openpyxl')
    records = []
    for sheet_name, df in xls.items():
        if df.empty:
            continue
        cols = {c: c.lower().strip() for c in df.columns}
        date_col = None
        name_col = None
        for c, lc in cols.items():
            for dh in DATE_HEADERS:
                if dh in lc:
                    date_col = c
                    break
            for nh in NAME_HEADERS:
                if nh in lc:
                    name_col = c
                    break
            if date_col and name_col:
                break
        if not date_col:
            date_col = df.columns[0]
        if not name_col and len(df.columns) > 1:
            name_col = df.columns[1]

        for _, row in df.iterrows():
            raw_date = row.get(date_col)
            d = try_parse_date(raw_date)
            if d is None:
                continue
            if name_col:
                raw_name = row.get(name_col)
                if pd.isna(raw_name):
                    continue
                name = str(raw_name).strip()
                if not name:
                    continue
                records.append({'date': d.isoformat(), 'name': name})
    logger.info('Parsed %d duty records', len(records))
    return records

# ========== СЕРВИС ОТПРАВКИ ==========
async def send_today_message():
    sel = get_config('selected_month')
    today = datetime.now(TIMEZONE).date()

    if sel:
        try:
            y, m = map(int, sel.split('-'))
        except Exception:
            y = None
            m = None
        if y and m:
            if today.year != y or today.month != m:
                return

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
            logger.exception('Не удалось отправить сообщение в %s: %s', chat_id, e)

# ========== ПЛАНИРОВЩИК ==========
scheduler = AsyncIOScheduler(timezone=TIMEZONE)

def schedule_daily(send_time: str):
    scheduler.remove_all_jobs()
    hh, mm = [int(x) for x in send_time.split(':')]
    trigger = CronTrigger(hour=hh, minute=mm)
    scheduler.add_job(lambda: asyncio.create_task(send_today_message()), trigger)
    logger.info('Scheduled daily job at %s', send_time)
    set_config('send_time', send_time)

# ========== МЕНЮ КОМАНД ==========
async def set_bot_commands():
    """Установка меню команд - разные команды для админов и не-админов"""
    # Команды для ВСЕХ пользователей
    all_commands = [
        types.BotCommand(command="start", description="ℹ️ Информация о боте"),
    ]
    
    # Команды только для АДМИНОВ
    admin_commands = [
        types.BotCommand(command="subscribe", description="📅 Добавить получателя"),
        types.BotCommand(command="unsubscribe", description="🚫 Удалить получателя"),
        types.BotCommand(command="send_today", description="📨 Отправить дежурных"),
        types.BotCommand(command="upload", description="📤 Загрузить Excel"),
        types.BotCommand(command="export", description="💾 Экспорт данных"),
        types.BotCommand(command="set_time", description="⏰ Установить время"),
        types.BotCommand(command="set_month", description="📆 Выбрать месяц"),
    ]
    
    # Устанавливаем команды для всех пользователей
    await bot.set_my_commands(all_commands)
    
    # Дополнительно устанавливаем все команды для админов
    for admin_id in ADMIN_IDS:
        try:
            await bot.set_my_commands(
                all_commands + admin_commands,
                scope=types.BotCommandScopeChat(chat_id=admin_id)
            )
        except Exception as e:
            logger.error(f"Не удалось установить команды для админа {admin_id}: {e}")

# ========== КОМАНДА /START (ДЛЯ ВСЕХ) ==========
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Команда start доступна всем пользователям"""
    is_admin = message.from_user.id in ADMIN_IDS
    
    if is_admin:
        welcome_text = (
            "👑 <b>Бот для рассылки дежурств (Администратор)</b>\n\n"
            "<i>Вы имеете полный доступ к управлению ботом.</i>\n\n"
            "📌 <b>Ваши команды:</b>\n"
            "• /subscribe - Добавить чат в получатели\n"
            "• /unsubscribe - Удалить чат из получателей\n"
            "• /send_today - Отправить сегодняшних дежурных\n"
            "• /set_time HH:MM - Установить время рассылки\n"
            "• /set_month YYYY-MM - Выбрать месяц рассылки\n"
            "• /upload - Загрузить Excel файл с дежурными\n"
            "• /export - Экспорт данных в Excel\n\n"
            "<i>Просто отправьте Excel файл для обновления списка дежурных.</i>"
        )
    else:
        # Обычный пользователь получает просто "помидор"
        welcome_text = "помидор"
    
    await message.reply(welcome_text)

# ========== КОМАНДЫ ТОЛЬКО ДЛЯ АДМИНОВ ==========
@dp.message(Command("subscribe"))
async def cmd_subscribe(message: types.Message):
    """Добавить чат в получатели - только для админов"""
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Эта команда доступна только администраторам.")
        return
    
    add_recipient(message.chat.id)
    await message.reply('✅ Этот чат добавлен в получатели рассылки.')

@dp.message(Command("unsubscribe"))
async def cmd_unsubscribe(message: types.Message):
    """Удалить чат из получателей - только для админов"""
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Эта команда доступна только администраторам.")
        return
    
    remove_recipient(message.chat.id)
    await message.reply('✅ Этот чат удален из получателей рассылки.')

@dp.message(Command("set_time"))
async def cmd_set_time(message: types.Message):
    """Установить время рассылки - только для админов"""
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Эта команда доступна только администраторам.")
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply('Использование: /set_time HH:MM')
        return
    
    t = parts[1].strip()
    try:
        hh, mm = map(int, t.split(':'))
        assert 0 <= hh < 24 and 0 <= mm < 60
    except Exception:
        await message.reply('❌ Неверный формат времени. Пример: /set_time 09:00')
        return
    
    schedule_daily(t)
    await message.reply(f'✅ Время отправки установлено на {t}')

@dp.message(Command("send_today"))
async def cmd_send_today(message: types.Message):
    """Отправить дежурных - только для админов"""
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Эта команда доступна только администраторам.")
        return
    
    await send_today_message()
    await message.reply('✅ Рассылка отправлена всем получателям.')

@dp.message(Command("set_month"))
async def cmd_set_month(message: types.Message):
    """Выбрать месяц - только для админов"""
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Эта команда доступна только администраторам.")
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply('Использование: /set_month YYYY-MM (например 2024-02)')
        return
    
    try:
        y, m = map(int, parts[1].split('-'))
        assert 1 <= m <= 12
    except Exception:
        await message.reply('❌ Неверный формат месяца. Пример: /set_month 2024-02')
        return
    
    set_config('selected_month', f"{y:04d}-{m:02d}")
    await message.reply(f'✅ Месяц рассылки установлен: {y:04d}-{m:02d}')

@dp.message(Command("export"))
async def cmd_export(message: types.Message):
    """Экспорт данных - только для админов"""
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("❌ Эта команда доступна только администраторам.")
        return
    
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query('SELECT duty_date, name FROM duties ORDER BY duty_date', conn)
    conn.close()
    out_path = os.path.join(DATA_DIR, f'duties_export_{datetime.now().strftime("%Y%m%d%H%M%S")}.xlsx')
    df.to_excel(out_path, index=False)
    await message.reply_document(types.FSInputFile(out_path))

# ========== ОБРАБОТКА ФАЙЛОВ (ТОЛЬКО ДЛЯ АДМИНОВ) ==========
@dp.message(F.document)
async def handle_docs(message: types.Message):
    """Загрузка Excel файлов - только для админов"""
    if message.from_user.id not in ADMIN_IDS:
        await message.reply('❌ Только администраторы могут загружать файлы.')
        return
    
    doc = message.document
    fname = doc.file_name or 'uploaded.xlsx'
    
    if not any(fname.lower().endswith(ext) for ext in ('.xls', '.xlsx', '.xlsm')):
        await message.reply('❌ Пожалуйста, загрузите файл Excel (.xls/.xlsx/.xlsm).')
        return
    
    saved_path = os.path.join(DATA_DIR, f'uploaded_{int(datetime.now().timestamp())}_{fname}')
    await bot.download(doc, destination=saved_path)
    await message.reply('📥 Файл получен, пытаюсь распарсить...')
    
    try:
        records = parse_excel(saved_path)
        if not records:
            await message.reply('❌ Не удалось распознать записи в файле. Проверьте формат.')
            return
        
        clear_duties()
        insert_duties(records)
        await message.reply(f'✅ Импортировано записей: {len(records)}')
        
    except Exception as e:
        logger.exception('Ошибка при парсинге файла: %s', e)
        await message.reply('❌ Произошла ошибка при обработке файла.')

# ========== ОБРАБОТКА ЛЮБЫХ ДРУГИХ СООБЩЕНИЙ ==========
@dp.message()
async def handle_other_messages(message: types.Message):
    """Обработка любых других сообщений"""
    if message.from_user.id not in ADMIN_IDS:
        # Для не-админов просто игнорируем или отправляем подсказку
        if message.text and message.text.startswith('/'):
            await message.reply(
                "❌ У вас нет доступа к этой команде.\n"
                "Доступные команды:\n"
                "• /start - Информация о боте\n\n"
                "Для доступа к функциям управления обратитесь к администратору."
            )
        return
    
    # Для админов - отправляем подсказку
    await message.reply(
        "👑 <b>Команды администратора:</b>\n\n"
        "• /start - Информация\n"
        "• /subscribe - Добавить получателя\n"
        "• /unsubscribe - Удалить получателя\n"
        "• /send_today - Отправить дежурных\n"
        "• /set_time HH:MM - Установить время\n"
        "• /set_month YYYY-MM - Выбрать месяц\n"
        "• /export - Экспорт данных\n"
        "• Отправьте Excel файл - Загрузить данные\n\n"
        "<i>Используйте команды или отправьте Excel файл</i>"
    )

# ========== ЗАПУСК БОТА ==========
async def on_startup():
    """Запуск при старте бота"""
    await set_bot_commands()
    
    send_time = get_config('send_time') or DEFAULT_SEND_TIME
    try:
        schedule_daily(send_time)
    except Exception as e:
        logger.exception('Не удалось запланировать задачу: %s', e)
    
    scheduler.start()
    logger.info('Бот запущен')

async def main():
    """Главная функция"""
    await on_startup()
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
