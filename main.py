"""
Telegram бот для рассылки дежурных с вебхуками для Render
"""

import os
import asyncio
import logging
import sqlite3
from datetime import datetime, date
from typing import List, Dict, Optional
import io
import csv

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import ContentType
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
from apscheduler.triggers.cron import CronTrigger
from aiohttp import web

# ========== НАСТРОЙКА ЛОГОВ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

TIMEZONE = pytz.timezone("Europe/Moscow")

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = [int(x.strip()) for x in os.getenv('ADMIN_IDS', '').split(',') if x.strip().isdigit()]
DEFAULT_SEND_TIME = os.getenv('DEFAULT_SEND_TIME', '09:00')
WEBHOOK_URL = os.getenv('WEBHOOK_URL')  # Например: https://your-bot.onrender.com
DATA_DIR = '/tmp/data'
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, 'duty_bot.db')

if not BOT_TOKEN:
    raise RuntimeError('❌ Установите BOT_TOKEN в переменных окружения Render')

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TIMEZONE)

# ========== БАЗА ДАННЫХ ==========
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
    logger.info("База данных инициализирована")

init_db()

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
    cur.execute('DELETE FROM recipients WHERE chat_id = ?', (chat_id,))
    conn.commit()
    conn.close()

def list_recipients() -> List[int]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT chat_id FROM recipients')
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]

def is_recipient(chat_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT 1 FROM recipients WHERE chat_id = ?', (chat_id,))
    result = cur.fetchone() is not None
    conn.close()
    return result

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

def get_all_duties() -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT duty_date, name FROM duties ORDER BY duty_date')
    rows = cur.fetchall()
    conn.close()
    return [{'date': r[0], 'name': r[1]} for r in rows]

def clear_all_duties():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('DELETE FROM duties')
    conn.commit()
    conn.close()

# ========== ПАРСЕР CSV ==========
def parse_csv(content: bytes) -> List[Dict]:
    """Парсит CSV файлы"""
    records = []
    try:
        text = content.decode('utf-8-sig', errors='ignore')
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        if len(lines) < 2:
            return records
        
        # Определяем разделитель
        first_line = lines[0]
        delimiter = ',' if ',' in first_line else (';' if ';' in first_line else '\t')
        
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        
        for row in reader:
            date_col = None
            name_col = None
            
            for col in row:
                col_lower = col.lower()
                if any(word in col_lower for word in ['дата', 'date', 'день']):
                    date_col = col
                elif any(word in col_lower for word in ['имя', 'фио', 'name', 'дежурный']):
                    name_col = col
            
            if date_col and name_col and row[date_col] and row[name_col]:
                date_str = row[date_col].strip()
                name = row[name_col].strip()
                
                try:
                    # Просто парсим дату как есть
                    if '-' in date_str:
                        d = datetime.strptime(date_str, '%Y-%m-%d').date()
                    elif '.' in date_str:
                        d = datetime.strptime(date_str, '%d.%m.%Y').date()
                    elif '/' in date_str:
                        d = datetime.strptime(date_str, '%d/%m/%Y').date()
                    else:
                        continue
                    
                    records.append({'date': d.isoformat(), 'name': name})
                except Exception as e:
                    logger.error(f"Ошибка парсинга даты '{date_str}': {e}")
                    continue
        
        logger.info(f"Парсинг CSV: найдено {len(records)} записей")
        
    except Exception as e:
        logger.error(f"Ошибка парсинга CSV: {e}")
    
    return records

# ========== РАССЫЛКА ==========
async def send_today_message():
    """Отправляет дежурных на сегодня"""
    try:
        today = datetime.now(TIMEZONE).date()
        names = get_duties_for_date(today)
        
        if not names:
            text = f'📅 На {today.strftime("%d.%m.%Y")} дежурных не найдено.'
        else:
            text = f'📅 Дежурные на {today.strftime("%d.%m.%Y")}:\n' + '\n'.join(f'• {n}' for n in names)
        
        recipients = list_recipients()
        if not recipients:
            recipients = ADMIN_IDS
        
        count = 0
        for chat_id in recipients:
            try:
                await bot.send_message(chat_id, text)
                count += 1
            except Exception as e:
                logger.error(f"Не удалось отправить в {chat_id}: {e}")
        
        logger.info(f"Рассылка отправлена {count} получателям")
        return count
        
    except Exception as e:
        logger.error(f"Ошибка в send_today_message: {e}")
        return 0

# ========== ПЛАНИРОВЩИК ==========
def schedule_daily(send_time: str):
    scheduler.remove_all_jobs()
    hh, mm = [int(x) for x in send_time.split(':')]
    trigger = CronTrigger(hour=hh, minute=mm)
    scheduler.add_job(lambda: asyncio.create_task(send_today_message()), trigger)
    logger.info(f'Рассылка запланирована на {send_time}')
    set_config('send_time', send_time)

# ========== СОЗДАНИЕ МЕНЮ КНОПОК ==========
def get_admin_menu():
    """Возвращает меню кнопок для администраторов"""
    buttons = [
        [types.KeyboardButton(text="📝 Подписаться на рассылку")],
        [types.KeyboardButton(text="❌ Отписаться от рассылки")],
        [types.KeyboardButton(text="📅 Дежурные сегодня")],
        [types.KeyboardButton(text="📋 Все дежурные")],
        [types.KeyboardButton(text="⚙️ Установить время")],
        [types.KeyboardButton(text="👥 Показать подписчиков")],
        [types.KeyboardButton(text="📤 Отправить сейчас")]
    ]
    
    return types.ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True
    )

# ========== КОМАНДЫ БОТА ==========
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    is_admin = message.from_user.id in ADMIN_IDS
    
    if is_admin:
        menu = get_admin_menu()
        await message.reply(
            "👑 <b>Бот для рассылки дежурных</b>\n\n"
            "<b>Доступные функции:</b>\n"
            "• Подписаться/отписаться от рассылки\n"
            "• Посмотреть дежурных сегодня\n"
            "• Посмотреть всех дежурных\n"
            "• Установить время рассылки\n"
            "• Показать список подписчиков\n"
            "• Отправить рассылку сейчас\n\n"
            "<b>Для загрузки данных отправьте CSV файл с колонками:</b>\n"
            "- Дата (ДД.ММ.ГГГГ или ГГГГ-ММ-ДД)\n"
            "- Имя (ФИО дежурного)",
            reply_markup=menu
        )
    else:
        await message.reply("помидор")

@dp.message(F.text == "📝 Подписаться на рассылку")
async def cmd_subscribe(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    chat_id = message.chat.id
    
    if is_recipient(chat_id):
        await message.reply("✅ Вы уже подписаны на рассылку!", reply_markup=get_admin_menu())
        return
    
    add_recipient(chat_id)
    send_time = get_config('send_time') or DEFAULT_SEND_TIME
    await message.reply(
        f"✅ Вы успешно подписались на рассылку!\n\n"
        f"Ежедневно в {send_time} вы будете получать список дежурных на текущий день.",
        reply_markup=get_admin_menu()
    )

@dp.message(F.text == "❌ Отписаться от рассылки")
async def cmd_unsubscribe(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    chat_id = message.chat.id
    
    if not is_recipient(chat_id):
        await message.reply("ℹ️ Вы не были подписаны на рассылку.", reply_markup=get_admin_menu())
        return
    
    remove_recipient(chat_id)
    await message.reply(
        "❌ Вы отписались от рассылки дежурных.",
        reply_markup=get_admin_menu()
    )

@dp.message(F.text == "📅 Дежурные сегодня")
async def cmd_duty_today(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    today = datetime.now(TIMEZONE).date()
    names = get_duties_for_date(today)
    
    if not names:
        text = f'📅 На {today.strftime("%d.%m.%Y")} дежурных не найдено.'
    else:
        text = f'📅 Дежурные на {today.strftime("%d.%m.%Y")}:\n' + '\n'.join(f'• {n}' for n in names)
    
    await message.reply(text, reply_markup=get_admin_menu())

@dp.message(F.text == "📋 Все дежурные")
async def cmd_all_duties(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    duties = get_all_duties()
    
    if not duties:
        await message.reply("📭 В базе данных нет записей о дежурных.", reply_markup=get_admin_menu())
        return
    
    # Группируем по дате
    duties_by_date = {}
    for duty in duties:
        date_str = duty['date']
        duties_by_date.setdefault(date_str, []).append(duty['name'])
    
    text = "📋 <b>Все дежурные:</b>\n\n"
    for date_str in sorted(duties_by_date.keys()):
        duty_date = datetime.fromisoformat(date_str).date()
        names = duties_by_date[date_str]
        text += f"<b>{duty_date.strftime('%d.%m.%Y')}:</b>\n"
        text += '\n'.join(f'• {n}' for n in names) + "\n\n"
    
    # Если текст слишком длинный, разбиваем на части
    if len(text) > 4000:
        parts = []
        current_part = ""
        for line in text.split('\n'):
            if len(current_part) + len(line) + 1 < 4000:
                current_part += line + "\n"
            else:
                parts.append(current_part)
                current_part = line + "\n"
        if current_part:
            parts.append(current_part)
        
        for part in parts:
            await message.reply(part)
    else:
        await message.reply(text, reply_markup=get_admin_menu())

@dp.message(F.text == "⚙️ Установить время")
async def cmd_set_time_menu(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    current_time = get_config('send_time') or DEFAULT_SEND_TIME
    await message.reply(
        f"⏰ Текущее время рассылки: <b>{current_time}</b>\n\n"
        "Для установки нового времени используйте команду:\n"
        "<code>/set_time HH:MM</code>\n\n"
        "Пример: <code>/set_time 09:00</code>",
        reply_markup=get_admin_menu()
    )

@dp.message(F.text == "👥 Показать подписчиков")
async def cmd_subscribers_menu(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    recipients = list_recipients()
    
    if not recipients:
        await message.reply("📭 Нет подписчиков на рассылку.", reply_markup=get_admin_menu())
        return
    
    text = f"📋 <b>Список подписчиков ({len(recipients)}):</b>\n\n"
    for chat_id in recipients:
        text += f"• ID: {chat_id}\n"
    
    await message.reply(text, reply_markup=get_admin_menu())

@dp.message(F.text == "📤 Отправить сейчас")
async def cmd_send_now(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    count = await send_today_message()
    await message.reply(f'✅ Рассылка отправлена {count} получателям', reply_markup=get_admin_menu())

@dp.message(Command("set_time"))
async def cmd_set_time(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply('Использование: /set_time HH:MM\nПример: /set_time 09:00')
        return
    
    t = parts[1].strip()
    try:
        hh, mm = map(int, t.split(':'))
        if not (0 <= hh < 24 and 0 <= mm < 60):
            raise ValueError
        schedule_daily(t)
        await message.reply(f'✅ Время рассылки установлено: {t}', reply_markup=get_admin_menu())
    except:
        await message.reply('❌ Неверный формат времени\nИспользуйте: HH:MM (например 09:00)', reply_markup=get_admin_menu())

@dp.message(Command("clear_duties"))
async def cmd_clear_duties(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    clear_all_duties()
    await message.reply('✅ Все записи о дежурных удалены.', reply_markup=get_admin_menu())

@dp.message(Command("subscribers"))
async def cmd_subscribers_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    recipients = list_recipients()
    
    if not recipients:
        await message.reply("📭 Нет подписчиков на рассылку.", reply_markup=get_admin_menu())
        return
    
    text = f"📋 <b>Список подписчиков ({len(recipients)}):</b>\n\n"
    for chat_id in recipients:
        text += f"• ID: {chat_id}\n"
    
    await message.reply(text, reply_markup=get_admin_menu())

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    help_text = (
        "👑 <b>Команды для администраторов:</b>\n\n"
        "<b>Кнопки меню:</b>\n"
        "• 📝 Подписаться на рассылку - подписаться\n"
        "• ❌ Отписаться от рассылки - отписаться\n"
        "• 📅 Дежурные сегодня - показать дежурных на сегодня\n"
        "• 📋 Все дежурные - показать все записи\n"
        "• ⚙️ Установить время - изменить время рассылки\n"
        "• 👥 Показать подписчиков - список подписчиков\n"
        "• 📤 Отправить сейчас - отправить рассылку немедленно\n\n"
        "<b>Текстовые команды:</b>\n"
        "• /set_time HH:MM - установить время рассылки\n"
        "• /clear_duties - очистить все записи о дежурных\n"
        "• /subscribers - показать подписчиков\n\n"
        "<b>Загрузка данных:</b>\n"
        "Отправьте CSV файл с колонками:\n"
        "- Дата (ДД.ММ.ГГГГ или ГГГГ-ММ-ДД)\n"
        "- Имя (ФИО дежурного)"
    )
    
    await message.reply(help_text, reply_markup=get_admin_menu())

@dp.message(F.document)
async def handle_docs(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")
        return
    
    doc = message.document
    fname = doc.file_name or 'uploaded.csv'
    
    if not any(fname.lower().endswith(ext) for ext in ('.csv', '.txt', '.xls', '.xlsx')):
        await message.reply('❌ Пожалуйста, загрузите CSV или текстовый файл', reply_markup=get_admin_menu())
        return
    
    await message.reply('📥 Файл получен, обработка...')
    
    try:
        file_data = io.BytesIO()
        await bot.download(doc, destination=file_data)
        content = file_data.getvalue()
        
        records = parse_csv(content)
        
        if not records:
            await message.reply(
                '❌ Не удалось распознать данные\n\n'
                'Формат CSV должен содержать колонки:\n'
                '- Дата (например: 01.02.2024 или 2024-02-01)\n'
                '- Имя (ФИО дежурного)',
                reply_markup=get_admin_menu()
            )
            return
        
        # Очищаем старые записи
        clear_all_duties()
        insert_duties(records)
        
        # Показываем пример данных
        sample_text = f'✅ Импортировано {len(records)} записей\n\n'
        
        # Группируем первые 5 дат для примера
        sample_records = records[:10]
        grouped = {}
        for record in sample_records:
            date_str = record['date']
            grouped.setdefault(date_str, []).append(record['name'])
        
        for date_str in sorted(grouped.keys())[:5]:  # Показываем максимум 5 дат
            duty_date = datetime.fromisoformat(date_str).date()
            names = grouped[date_str]
            sample_text += f"<b>{duty_date.strftime('%d.%m.%Y')}:</b>\n"
            sample_text += '\n'.join(f'• {n}' for n in names[:3])  # Показываем максимум 3 имени на дату
            if len(names) > 3:
                sample_text += f'\n... и еще {len(names) - 3} дежурных\n'
            sample_text += "\n"
        
        if len(records) > 10:
            sample_text += f"\n... и еще {len(records) - 10} записей\n"
        
        sample_text += "\nИспользуйте кнопку '📅 Дежурные сегодня' для проверки"
        
        await message.reply(sample_text, reply_markup=get_admin_menu())
        
    except Exception as e:
        logger.error(f"Ошибка обработки файла: {e}")
        await message.reply('❌ Произошла ошибка при обработке файла', reply_markup=get_admin_menu())

# Обработка всех остальных сообщений от не-администраторов
@dp.message()
async def handle_non_admin_messages(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("помидор")

# ========== ВЕБХУКИ И HTTP СЕРВЕР ==========
async def handle_health(request):
    """Health check эндпоинт"""
    return web.Response(text="OK")

async def handle_trigger(request):
    """Ручной запуск рассылки (для cron)"""
    # Простая проверка токена (опционально)
    token = request.headers.get('X-Auth-Token')
    expected_token = os.getenv('CRON_TOKEN', 'default-secret')
    
    if token != expected_token:
        return web.Response(text="Unauthorized", status=401)
    
    count = await send_today_message()
    return web.Response(text=f"✅ Рассылка отправлена {count} получателям")

async def handle_home(request):
    """Главная страница"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>🤖 Telegram Duty Bot</title>
        <meta charset="utf-8">
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; max-width: 800px; margin: 0 auto; padding: 20px; }
            .status { padding: 10px; background: #4CAF50; color: white; border-radius: 5px; }
            .endpoints { margin-top: 20px; }
            ul { line-height: 1.6; }
        </style>
    </head>
    <body>
        <h1>🤖 Telegram Duty Bot</h1>
        <div class="status">✅ Сервис работает</div>
        <p>Бот для автоматической рассылки дежурных в Telegram.</p>
        
        <div class="endpoints">
            <h3>📡 Конечные точки:</h3>
            <ul>
                <li><strong>GET</strong> <a href="/">/</a> - Эта страница</li>
                <li><strong>GET</strong> <a href="/health">/health</a> - Проверка работоспособности</li>
                <li><strong>POST</strong> /webhook - Вебхук для Telegram (скрытый)</li>
                <li><strong>POST</strong> /trigger - Ручной запуск рассылки (требует X-Auth-Token)</li>
            </ul>
        </div>
        
        <div style="margin-top: 30px; padding: 15px; background: #f5f5f5; border-radius: 5px;">
            <h3>⚙️ Настройка автоматической рассылки:</h3>
            <p>Используйте cron-сервис для ежедневной отправки:</p>
            <code>POST https://ваш-бот.onrender.com/trigger</code><br>
            <code>Header: X-Auth-Token: ваш-секретный-токен</code>
        </div>
    </body>
    </html>
    """
    return web.Response(text=html, content_type='text/html')

async def on_startup():
    """Настройка при запуске"""
    # Настраиваем вебхук если указан URL
    if WEBHOOK_URL:
        webhook_path = f"{WEBHOOK_URL}/webhook"
        await bot.set_webhook(webhook_path)
        logger.info(f"Вебхук установлен: {webhook_path}")
    else:
        logger.warning("WEBHOOK_URL не указан, вебхук не установлен")
    
    # Настраиваем расписание
    send_time = get_config('send_time') or DEFAULT_SEND_TIME
    try:
        schedule_daily(send_time)
        scheduler.start()
        logger.info(f"Планировщик запущен. Рассылка в {send_time}")
    except Exception as e:
        logger.error(f"Ошибка планирования: {e}")

async def main():
    """Главная функция с вебхуками"""
    # Создаем aiohttp приложение
    app = web.Application()
    
    # Регистрируем вебхук для Telegram
    webhook_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )
    webhook_handler.register(app, path="/webhook")
    
    # Регистрируем остальные маршруты
    app.router.add_get("/", handle_home)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/trigger", handle_trigger)
    
    # Настраиваем приложение
    setup_application(app, dp, bot=bot)
    
    # Инициализация
    await on_startup()
    
    # Запускаем сервер
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    
    logger.info(f"🚀 Сервер запущен на порту {port}")
    
    # Бесконечный запуск
    await site.start()
    
    # Держим сервер активным
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка сервера...")
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
