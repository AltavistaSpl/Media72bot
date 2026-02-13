# ==============================
# 1. ИМПОРТЫ И КОНФИГУРАЦИЯ
# ==============================
import sqlite3
import threading
import telebot
import time
from telebot import types
from datetime import datetime, timedelta
import pandas as pd
import os
import traceback
import re
import hashlib

# Отключаем SSL проверку для requests
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

from config import bot, ADMIN_IDS, RULES_TEXT, EXCEL_FILE_PATH
from spiski import AVAILABLE_CITIES, ACHIEVEMENT_EMOJIS, STICKER_IDS, ACHIEVEMENT_MESSAGES, COUNTERS_CONFIG

TASKS_PER_PAGE = 5  # Количество задач на одной странице

# Глобальный словарь для кэша рассылки
broadcast_cache = {}
# Создаем локальную переменную для потоков
thread_local = threading.local()



# ==============================
# 2. БАЗА ДАННЫХ
# ==============================
def get_db_connection():
    """Потокобезопасное соединение с БД"""
    if not hasattr(thread_local, 'connection'):
        thread_local.connection = sqlite3.connect('users.db', check_same_thread=False)
        thread_local.connection.row_factory = sqlite3.Row
        thread_local.connection.execute("PRAGMA foreign_keys = ON")
    return thread_local.connection
def init_db():
    """Инициализация базы данных"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Таблица распуш-задач
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raspush_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT,
            task_description TEXT,
            created_at TEXT,
            expires_at TEXT
        )
    ''')

    # Таблица выполнений распуша
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raspush_completions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            user_id INTEGER,
            city TEXT,
            links TEXT,
            completed_at TEXT,
            UNIQUE(task_id, city)
        )
    ''')

    # Таблица пользователей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            city TEXT DEFAULT 'Не указан',
            points INTEGER DEFAULT 0,
            registration_date TEXT,
            last_active TEXT,
            is_banned INTEGER DEFAULT 0
        )
    ''')

    # Таблица истории операций
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS points_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount INTEGER,
            reason TEXT,
            admin_id INTEGER,
            date TEXT,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    ''')

    # Таблица настроек бота
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
    ''')

    # Таблица счётчиков пользователей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_counters (
            user_id INTEGER,
            counter_type TEXT,
            value INTEGER DEFAULT 0,
            last_updated TEXT,
            PRIMARY KEY (user_id, counter_type),
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    ''')

    # Создаем таблицу задач
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bot_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_name TEXT NOT NULL,
            task_description TEXT,
            assigned_city TEXT NOT NULL,
            assigned_by_admin INTEGER,
            assigned_date TEXT,
            due_date TEXT,
            is_completed BOOLEAN DEFAULT 0,
            completed_date TEXT,
            points_reward INTEGER DEFAULT 0,
            is_all_cities BOOLEAN DEFAULT 0,
            deadline_notified BOOLEAN DEFAULT 0,
            FOREIGN KEY (assigned_by_admin) REFERENCES users (user_id)
        )
    ''')

    # Попытка добавить колонку если она не существует (безопасный вариант)
    try:
        cursor.execute('''
            ALTER TABLE bot_tasks 
            ADD COLUMN deadline_notified BOOLEAN DEFAULT 0
        ''')
    except sqlite3.OperationalError:
        pass  # Колонка уже существует

    # Добавляем колонку для распуш-задач
    try:
        cursor.execute('''
            ALTER TABLE bot_tasks 
            ADD COLUMN is_raspush BOOLEAN DEFAULT 0
        ''')
    except sqlite3.OperationalError:
        pass


    # Таблица достижений пользователей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_achievements (
            user_id INTEGER,
            achievement_id TEXT,
            unlocked_at TEXT,
            is_manual BOOLEAN DEFAULT 0,
            admin_id INTEGER DEFAULT NULL,
            notified BOOLEAN DEFAULT 0,
            PRIMARY KEY (user_id, achievement_id),
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    ''')

    # Таблица истории всех достижений
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS achievements_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            achievement_id TEXT,
            unlocked_at TEXT,
            is_manual BOOLEAN DEFAULT 0,
            admin_id INTEGER DEFAULT NULL,
            reason TEXT DEFAULT '',
            points_awarded INTEGER DEFAULT 5,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    ''')

    # Таблица истории планёрок
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS meetings_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            meeting_date TEXT,
            meeting_topic TEXT,
            added_by_admin INTEGER,
            notes TEXT,
            created_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    ''')

    conn.commit()

# ==============================
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==============================
def get_or_create_user(user_id, username, first_name, last_name, city='Не указан'):
    """Получить или создать пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    if not cursor.fetchone():
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''
            INSERT INTO users (user_id, username, first_name, last_name, city, 
                               points, registration_date, last_active, is_banned)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, username or '', first_name or '', last_name or '',
              city, 0, now, now, 0))
        conn.commit()
    return True
def get_user_info(user_id):
    """Информация о пользователе"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    return cursor.fetchone()
def update_user_points(user_id, amount):
    """Обновление баллов пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('UPDATE users SET points = points + ? WHERE user_id = ?', (amount, user_id))
    cursor.execute('SELECT points FROM users WHERE user_id = ?', (user_id,))
    new_points = cursor.fetchone()['points']

    conn.commit()
    return new_points
def log_points_history(user_id, amount, reason, admin_id):
    """Логирование операций"""
    conn = get_db_connection()
    cursor = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute('''
        INSERT INTO points_history (user_id, amount, reason, admin_id, date)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_id, amount, reason, admin_id, now))

    conn.commit()
def update_user_city(user_id, city):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET city = ? WHERE user_id = ?', (city, user_id))
    conn.commit()
    return True
def is_admin(user_id):
    """Проверка прав администратора"""
    return user_id in ADMIN_IDS
def ensure_tables_exist():
    """Проверить и создать необходимые таблицы, если их нет"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Проверяем существование таблицы points_history
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='points_history'")
    if not cursor.fetchone():
        cursor.execute('''
            CREATE TABLE points_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount INTEGER,
                reason TEXT,
                admin_id INTEGER,
                date TEXT,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
    conn.commit()

# Правила и Контент-план
def save_rules(rules_text):
    """Сохранить правила в базе данных"""
    conn = get_db_connection()
    cursor = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute('''
        INSERT OR REPLACE INTO bot_settings (key, value, updated_at)
        VALUES (?, ?, ?)
    ''', ('rules', rules_text, now))

    conn.commit()
def save_content_plan_info(message):
    """Сохранить информацию о контент-плане"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Сохраняем file_id изображения
    file_id = None
    if message.photo:
        # Берем самое большое изображение (последнее в списке)
        file_id = message.photo[-1].file_id
    elif message.document:
        file_id = message.document.file_id

    caption = message.caption or "📅 Контент-план"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Сохраняем file_id если есть
    if file_id:
        cursor.execute('''
            INSERT OR REPLACE INTO bot_settings (key, value, updated_at)
            VALUES (?, ?, ?)
        ''', ('content_plan_file_id', file_id, now))

    # Сохраняем подпись
    cursor.execute('''
        INSERT OR REPLACE INTO bot_settings (key, value, updated_at)
        VALUES (?, ?, ?)
    ''', ('content_plan_caption', caption, now))

    conn.commit()
    return file_id, caption
def get_rules():
    """Получить правила работы"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT value FROM bot_settings WHERE key = ?', ('rules',))
    result = cursor.fetchone()

    if result:
        return result['value']
    else:
        # Правила по умолчанию
        return """📋 <b>Правила работы:</b>

1. Соблюдайте сроки публикаций
2. Проверяйте информацию на достоверность
3. Согласовывайте материалы с редактором
4. Соблюдайте стилистику издания
5. Ведёте журнал выполненных работ

По всем вопросам обращайтесь к администратору."""
def get_content_plan_info():
    """Получить информацию о контент-плане"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT value FROM bot_settings WHERE key = ?', ('content_plan_file_id',))
    file_id_result = cursor.fetchone()

    cursor.execute('SELECT value FROM bot_settings WHERE key = ?', ('content_plan_caption',))
    caption_result = cursor.fetchone()

    return {
        'file_id': file_id_result['value'] if file_id_result else None,
        'caption': caption_result['value'] if caption_result else "📅 Контент-план"
    }

#Рейтинг
def get_city_rating():
    """Получить рейтинг муниципалитетов по среднему баллу"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT 
            city,
            COUNT(*) as users_count,
            SUM(points) as total_points,
            ROUND(AVG(points), 1) as avg_points,
            MAX(points) as max_points
        FROM users 
        WHERE city != 'Не указан' AND is_banned = 0
        GROUP BY city 
        ORDER BY avg_points DESC, total_points DESC
    ''')

    return cursor.fetchall()
def show_city_rating(chat_id, message_id=None):
    """Показать рейтинг муниципалитетов"""
    rating = get_city_rating()

    if not rating:
        response = "🏆 <b>Рейтинг муниципалитетов</b>\n\nПока нет данных для рейтинга."
    else:
        response = "🏆 <b>РЕЙТИНГ МУНИЦИПАЛИТЕТОВ</b>\n\n"

        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

        for i, city in enumerate(rating[:10], 1):
            medal = medals[i - 1] if i <= 10 else f"{i}."
            city_emoji = AVAILABLE_CITIES.get(city['city'], '🏙️')

            response += (
                f"{medal} {city['city']} | {city['total_points']} баллов\n"
            )

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='personal_cabinet'))

    if message_id:
        bot.edit_message_text(response, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
    else:
        bot.send_message(chat_id, response, parse_mode='HTML', reply_markup=markup)
def show_city_stats_for_admin(chat_id):
    """Расширенная статистика для админа"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Топ муниципалитетов по активным задачам
    cursor.execute('''
        SELECT assigned_city, COUNT(*) as active_tasks
        FROM bot_tasks 
        WHERE is_completed = 0
        GROUP BY assigned_city 
        ORDER BY active_tasks DESC
        LIMIT 5
    ''')
    top_tasks = cursor.fetchall()

    # Рейтинг
    rating = get_city_rating()

    response = "📊 <b>СТАТИСТИКА МУНИЦИПАЛИТЕТОВ</b>\n\n"

    if rating:
        response += "<b>🏆 ТОП-5:</b>\n"
        for i, city in enumerate(rating[:5], 1):
            city_emoji = AVAILABLE_CITIES.get(city['city'], '🏙️')
            response += f"{i}. {city_emoji} {city['city']}: {city['avg_points']} баллов\n"
        response += "\n"

    bot.send_message(chat_id, response, parse_mode='HTML')

def make_task_uid(task_name: str) -> str:
    """
    Генерирует короткий безопасный ID задачи (≤ 64 байт)
    """
    return hashlib.md5(task_name.encode('utf-8')).hexdigest()[:16]
# ==========================
# ЭКСЕЛЬ ФУНКЦИИ
# ==========================
def load_tasks_from_excel():
    """Загрузить задачи из Excel файла"""
    try:
        if not os.path.exists(EXCEL_FILE_PATH):
            return None, "Файл с задачами не найден"

        # Принудительно читаем все столбцы как строки, но с сохранением дат
        # Используем converters для конкретных столбцов
        converters = {
            'Дата': str,  # Даты читаем как строки
            'Задача': str,  # Задачи как строки
            'Описание': str,  # Описания как строки
            'Ответственный': str  # Ответственный как строки
        }

        df = pd.read_excel(
            EXCEL_FILE_PATH,
            engine='openpyxl',
            converters=converters,  # Важно: converters преобразует данные при чтении
            dtype=None  # Отключаем автоматическое определение типов
        )

        # Принудительно преобразуем все нужные столбцы в строки (на всякий случай)
        for col in ['Дата', 'Задача', 'Описание', 'Ответственный']:
            if col in df.columns:
                # Преобразуем все в строки, заменяя NaN, None, 'nan' на пустую строку
                df[col] = df[col].astype(str).replace({
                    'nan': '',
                    'None': '',
                    'NaN': '',
                    '<NA>': '',
                    'NaT': '',
                    '': ''
                }).str.strip()

        # Теперь обработка дат
        for i in range(len(df)):
            date_str = df.at[i, 'Дата']

            if not date_str or date_str in ['', 'nan', 'None', 'NaT', '<NA>']:
                df.at[i, 'Дата'] = ""
            else:
                # Убираем возможные лишние символы
                date_str = str(date_str).strip()

                # Если это уже отформатированная дата, оставляем как есть
                if re.match(r'\d{2}\.\d{2}\.\d{4}', date_str):
                    df.at[i, 'Дата'] = date_str
                else:
                    # Пробуем разные форматы
                    try:
                        formats = [
                            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                            "%d.%m.%Y", "%m/%d/%Y", "%d-%m-%Y",
                            "%Y/%m/%d", "%d.%m.%y"
                        ]
                        parsed = False
                        for fmt in formats:
                            try:
                                dt = datetime.strptime(date_str, fmt)
                                df.at[i, 'Дата'] = dt.strftime("%d.%m.%Y")
                                parsed = True
                                break
                            except ValueError:
                                continue
                        if not parsed:
                            # Оставляем как есть
                            df.at[i, 'Дата'] = date_str
                    except:
                        df.at[i, 'Дата'] = date_str

        # Обработка столбца "Ответственный"
        if 'Ответственный' in df.columns:
            df['Ответственный'] = df['Ответственный'].replace({
                'nan': '', 'None': '', 'NaN': '', '': ''
            }).str.strip()

            # Заменяем ключевые слова
            replace_dict = {
                'MUNICIPALITIES': 'Все муниципалитеты',
                'ALL': 'Все муниципалитеты',
                'all municipalities': 'Все муниципалитеты',
                'all': 'Все муниципалитеты',
                'All': 'Все муниципалитеты',
                'ВСЕ': 'Все муниципалитеты',
                'Все': 'Все муниципалитеты'
            }

            for old, new in replace_dict.items():
                # Заменяем только если строка полностью совпадает
                mask = df['Ответственный'].astype(str).str.strip() == old
                df.loc[mask, 'Ответственный'] = new

        # Проверяем наличие необходимых столбцов
        required_columns = ['Дата', 'Задача', 'Описание', 'Ответственный']
        for col in required_columns:
            if col not in df.columns:
                return None, f"Отсутствует столбец: {col}"

        tasks = df.to_dict('records')
        return tasks, None

    except Exception as e:
        import traceback
        return None, f"Ошибка при загрузке файла: {str(e)}\n\n{traceback.format_exc()}"
def filter_tasks_by_city(tasks, city_name):
    """Отфильтровать задачи по муниципалитету (по ответственному)"""
    if not tasks:
        return []

    # ЕСЛИ ЗАДАЧА ДЛЯ ВСЕХ МУНИЦИПАЛИТЕТОВ
    if city_name == "ALL" or city_name == "Все муниципалитеты":
        # Показываем ВСЕ задачи, которые не пустые
        filtered_tasks = []
        for task in tasks:
            if task.get('Ответственный'):
                filtered_tasks.append(task)
        return filtered_tasks

    city_lower = str(city_name).lower()
    filtered_tasks = []

    for task in tasks:
        if task.get('Ответственный'):
            # Если ответственный содержит "Все муниципалитеты" или "ALL" - показываем всем
            if 'все муниципалитеты' in str(task['Ответственный']).lower() or 'all' in str(
                    task['Ответственный']).lower():
                filtered_tasks.append(task)
            elif city_lower in str(task['Ответственный']).lower():
                filtered_tasks.append(task)

    def date_key(task):
        date_str = task.get('Дата', '')
        if not date_str:
            return datetime.max
        try:
            # Преобразуем разные форматы дат
            formats = ["%d.%m.%Y", "%Y-%m-%d", "%m/%d/%Y"]
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            return datetime.max
        except:
            return datetime.max

    filtered_tasks.sort(key=date_key)

    return filtered_tasks
def show_user_tasks_by_city(user_id, chat_id, page=0, message_id=None):
    """Показать задачи пользователя (из Excel + из бота)"""
    user = get_user_info(user_id)
    if not user:
        bot.send_message(chat_id, "❌ Пользователь не найден")
        return

    user_city = user['city']

    # Загружаем ВСЕ задачи из Excel
    all_tasks, error = load_tasks_from_excel()
    if error:
        bot.send_message(chat_id, f"❌ {error}")
        return

    # Фильтруем задачи по муниципалитету
    city_tasks = filter_tasks_by_city(all_tasks, user_city)

    if not city_tasks:
        response = (
            f"📋 <b>Мои задачи ({user_city})</b>\n\n"
            f"Для вашего муниципалитета ({user_city}) пока нет задач.\n\n"
            f"<i>В файле задач в поле 'Ответственный' должен быть указан: {user_city}</i>"
        )

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='personal_cabinet'))

        if message_id:
            bot.edit_message_text(response, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
        else:
            bot.send_message(chat_id, response, parse_mode='HTML', reply_markup=markup)
        return

    # Пагинация
    total_tasks = len(city_tasks)
    total_pages = (total_tasks + TASKS_PER_PAGE - 1) // TASKS_PER_PAGE

    start_idx = page * TASKS_PER_PAGE
    end_idx = min(start_idx + TASKS_PER_PAGE, total_tasks)
    current_tasks = city_tasks[start_idx:end_idx]

    # Формируем ответ
    response = (
        f"📋 <b>Мои задачи ({user_city})</b>\n\n"
        f"<i>Всего задач: {total_tasks}</i>\n"
        f"<i>Страница {page + 1}/{total_pages}</i>\n\n"
    )

    for i, task in enumerate(current_tasks, start_idx + 1):
        response += (
            f"<b>{i}. {task['Задача']}</b>\n"
            f"📅 {task['Дата']}\n"
            f"{'-' * 30}\n"
        )

    # Создаем клавиатуру
    markup = types.InlineKeyboardMarkup(row_width=3)

    # Кнопки для детального просмотра задач
    task_buttons = []
    for i, task in enumerate(current_tasks):
        # Используем относительный индекс от 0 до TASKS_PER_PAGE-1
        relative_index = i
        task_buttons.append(
            types.InlineKeyboardButton(
                f"📄 {start_idx + i + 1}",
                callback_data=f'show_city_task_detail_{relative_index}_{page}'
            )
        )

    # Разбиваем на строки
    for i in range(0, len(task_buttons), 5):
        markup.add(*task_buttons[i:i + 5])

    # Кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(types.InlineKeyboardButton('⬅️ Назад', callback_data=f'city_tasks_page_{page - 1}'))

    nav_buttons.append(types.InlineKeyboardButton('🔙 В кабинет', callback_data='personal_cabinet'))

    if page < total_pages - 1:
        nav_buttons.append(types.InlineKeyboardButton('Далее ➡️', callback_data=f'city_tasks_page_{page + 1}'))

    markup.add(types.InlineKeyboardButton('🚀 Задачи РАСПУШ', callback_data='raspush_my_tasks_0'))
    markup.add(*nav_buttons)

    # Отправляем/редактируем
    if message_id:
        bot.edit_message_text(response, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
    else:
        bot.send_message(chat_id, response, parse_mode='HTML', reply_markup=markup)
def show_task_detail_by_city(user_id, chat_id, relative_index, page_context, message_id=None):
    """Показать детальное описание задачи для муниципалитета"""
    user = get_user_info(user_id)
    if not user:
        bot.send_message(chat_id, "❌ Пользователь не найден")
        return

    user_city = user['city']

    # Загружаем ВСЕ задачи
    tasks, error = load_tasks_from_excel()
    if error:
        bot.send_message(chat_id, f"❌ {error}")
        return

    # Фильтруем по муниципалитету
    city_tasks = filter_tasks_by_city(tasks, user_city)

    # Вычисляем абсолютный индекс с учетом страницы
    absolute_index = (page_context * TASKS_PER_PAGE) + relative_index  # ← ВАЖНО

    if not city_tasks or absolute_index >= len(city_tasks):
        bot.send_message(chat_id, "❌ Задача не найдена")
        return

    task = city_tasks[absolute_index]  # ← Используем абсолютный инде

    # Формируем детальный ответ
    response = (
        f"<b>📋 Задача для {user_city}</b>\n\n"
        f"<b>Название:</b> {task['Задача']}\n"
        f"<b>Дата:</b> {task['Дата']}\n"
        f"<b>Ответственный:</b> {task['Ответственный']}\n"
        f"<b>Описание:</b>\n{task['Описание']}\n\n"
        f"{'=' * 40}\n"
    )

    # Создаем клавиатуру для возврата
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('🔙 К моим задачам', callback_data=f'city_tasks_page_{page_context}'))

    if message_id:
        bot.edit_message_text(response, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
    else:
        bot.send_message(chat_id, response, parse_mode='HTML', reply_markup=markup)
def show_all_tasks(chat_id, page=0, message_id=None):
    """Показать ВСЕ задачи из файла Excel"""
    tasks, error = load_tasks_from_excel()
    if error:
        if message_id:
            bot.edit_message_text(f"❌ {error}", chat_id, message_id, parse_mode='HTML')
        else:
            bot.send_message(chat_id, f"❌ {error}", parse_mode='HTML')
        return

    if not tasks:
        response = "📋 <b>Список всех задач</b>\n\nВ файле пока нет задач."
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('🔙 В кабинет', callback_data='personal_cabinet'))

        if message_id:
            bot.edit_message_text(response, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
        else:
            bot.send_message(chat_id, response, parse_mode='HTML', reply_markup=markup)
        return

    def date_key(task):
        date_str = task.get('Дата', '')
        if not date_str:
            return datetime.max
        try:
            formats = ["%d.%m.%Y", "%Y-%m-%d", "%m/%d/%Y"]
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            return datetime.max
        except:
            return datetime.max

    # СОХРАНИТЬ ИСХОДНЫЕ ИНДЕКСЫ ДО СОРТИРОВКИ
    tasks_with_original_index = list(enumerate(tasks))  # [(0, task1), (1, task2), ...]

    # Сортировка с сохранением оригинальных индексов
    tasks_with_original_index.sort(key=lambda x: date_key(x[1]))

    # Разделяем отсортированные индексы и задачи
    sorted_indices = [idx for idx, _ in tasks_with_original_index]
    sorted_tasks = [task for _, task in tasks_with_original_index]

    # Пагинация для всех задач
    total_tasks = len(sorted_tasks)
    total_pages = (total_tasks + TASKS_PER_PAGE - 1) // TASKS_PER_PAGE

    start_idx = page * TASKS_PER_PAGE
    end_idx = min(start_idx + TASKS_PER_PAGE, total_tasks)
    current_tasks = sorted_tasks[start_idx:end_idx]
    current_indices = sorted_indices[start_idx:end_idx]  # ← Важно: используем оригинальные индексы!

    # Формируем ответ
    response = (
        f"📋 <b>СПИСОК ВСЕХ ЗАДАЧ</b>\n\n"
        f"<i>Всего задач: {total_tasks}</i>\n"
        f"<i>Страница {page + 1}/{total_pages}</i>\n\n"
    )

    # ИСПРАВЛЕННАЯ ЧАСТЬ: показываем оригинальные номера задач
    for display_number, (task, original_idx) in enumerate(zip(current_tasks, current_indices), 1):
        response += (
            f"<b>{original_idx + 1}. {task['Задача']}</b>\n"  # ← Используем original_idx + 1
            f"📅 {task['Дата']} | 👤 {('Не назначен' if pd.isna(task.get('Ответственный')) else task['Ответственный'])}\n"
            f"{'-' * 30}\n"
        )

    # Создаем клавиатуру
    markup = types.InlineKeyboardMarkup(row_width=3)

    # Кнопки для детального просмотра - используем ОРИГИНАЛЬНЫЕ индексы
    task_buttons = []
    for task, original_idx in zip(current_tasks, current_indices):
        task_buttons.append(
            types.InlineKeyboardButton(
                f"📄 {original_idx + 1}",  # ← Тоже original_idx + 1
                callback_data=f'show_all_task_detail_{original_idx}'
            )
        )

    # Разбиваем на строки
    for i in range(0, len(task_buttons), 5):
        markup.add(*task_buttons[i:i + 5])

    # Кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(types.InlineKeyboardButton('⬅️ Назад', callback_data=f'all_tasks_page_{page - 1}'))

    nav_buttons.append(types.InlineKeyboardButton('🔙 В кабинет', callback_data='personal_cabinet'))

    if page < total_pages - 1:
        nav_buttons.append(types.InlineKeyboardButton('Далее ➡️', callback_data=f'all_tasks_page_{page + 1}'))

    markup.add(*nav_buttons)

    # Отправляем/редактируем
    if message_id:
        bot.edit_message_text(response, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
    else:
        bot.send_message(chat_id, response, parse_mode='HTML', reply_markup=markup)
def show_task_detail_all(chat_id, task_index, page_context=None, message_id=None):
    """Показать детали задачи из общего списка"""
    tasks, error = load_tasks_from_excel()
    if error:
        bot.send_message(chat_id, f"❌ {error}")
        return

    if not tasks or task_index >= len(tasks):
        bot.send_message(chat_id, "❌ Задача не найдена")
        return

    task = tasks[task_index]

    # Проверяем, свободна ли задача
    current_responsible = str(task.get('Ответственный', '')).strip().lower()

    # Список значений, которые считаем "пустыми"
    empty_values = ['', 'nan', 'none', 'nat', '<na>', 'не назначен']
    is_free = current_responsible in empty_values or pd.isna(current_responsible)

    # Формируем детальный ответ
    response = (
        f"<b>📋 Задача #{task_index + 1}</b>\n\n"
        f"<b>Название:</b> {task['Задача']}\n"
        f"<b>Дата:</b> {task['Дата']}\n"
        f"<b>Ответственный:</b> {current_responsible.title() if current_responsible and current_responsible not in empty_values else '❌ Не назначен'}\n"
        f"<b>Описание:</b>\n{task['Описание']}\n\n"
    )

    if is_free:
        response += "✅ <i>Эта задача свободна и доступна для принятия</i>\n\n"

    response += f"{'=' * 40}\n"

    # Создаем клавиатуру
    markup = types.InlineKeyboardMarkup()

    if is_free:
        task_uid = make_task_uid(task['Задача'])

        markup.add(
            types.InlineKeyboardButton(
                '✅ Принять задачу',
                callback_data=f'accept_task:{task_uid}'
            )
        )
    if page_context is not None:
        markup.add(types.InlineKeyboardButton('🔙 К списку задач', callback_data=f'all_tasks_page_{page_context}'))
    else:
        markup.add(types.InlineKeyboardButton('🔙 К списку задач', callback_data='all_tasks_list'))

    if message_id:
        bot.edit_message_text(response, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
    else:
        bot.send_message(chat_id, response, parse_mode='HTML', reply_markup=markup)
def add_task_to_excel(task_name, description, assigned_city, due_date=None):
    """Добавить задачу в Excel файл"""
    try:
        file_path = EXCEL_FILE_PATH

        if not os.path.exists(file_path):
            return False, "Файл не найден"

        df = pd.read_excel(file_path)

        if due_date:
            due_date_str = due_date.strftime("%d.%m.%Y")
        else:
            due_date_str = ""

        # ИСПРАВЬ ЭТУ СТРОКУ: замени "ALL" на "Все муниципалитеты"
        display_city = "Все муниципалитеты" if assigned_city == "Все муниципалитеты" else assigned_city

        # Создаем новую строку
        new_task = {
            'Дата': due_date_str,
            'Задача': task_name,
            'Описание': description,
            'Ответственный': display_city  # ← ЗДЕСЬ ИСПРАВЛЕНО
        }

        df = pd.concat([df, pd.DataFrame([new_task])], ignore_index=True)
        df.to_excel(file_path, index=False)

        return True, "Задача добавлена в Excel"

    except Exception as e:
        return False, f"Ошибка при записи в Excel: {str(e)}"
def accept_task_by_uid(task_uid, user_id):
    try:
        df = pd.read_excel(EXCEL_FILE_PATH, engine='openpyxl')

        # Ищем задачу по UID
        for idx, row in df.iterrows():
            uid = make_task_uid(str(row['Задача']))
            if uid == task_uid:
                user = get_user_info(user_id)
                if not user:
                    return False, "❌ Пользователь не найден"

                df.at[idx, 'Ответственный'] = user['city']
                df.to_excel(EXCEL_FILE_PATH, index=False)

                return True, f"✅ Задача принята!\n📍 {user['city']}"

        return False, "❌ Задача не найдена"

    except Exception as e:
        return False, f"❌ Ошибка: {str(e)}"

#Формирование отчёта
def generate_points_history_report(start_date=None, end_date=None):
    """Сгенерировать отчет по истории начислений баллов"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Формируем запрос с фильтрацией по дате
        query = '''
            SELECT 
                ph.date,
                ph.user_id,
                u.first_name,
                u.city,
                ph.amount,
                ph.reason,
                ph.admin_id,
                a.first_name as admin_name
            FROM points_history ph
            LEFT JOIN users u ON ph.user_id = u.user_id
            LEFT JOIN users a ON ph.admin_id = a.user_id
            WHERE 1=1
        '''
        params = []

        if start_date:
            query += " AND ph.date >= ?"
            params.append(start_date)

        if end_date:
            query += " AND ph.date <= ?"
            params.append(end_date)

        query += " ORDER BY ph.date DESC"

        cursor.execute(query, params)
        history = cursor.fetchall()

        if not history:
            return None, "Нет данных за указанный период"

        # Создаем DataFrame
        data = []
        for row in history:
            data.append({
                'Дата': row['date'],
                'ID пользователя': row['user_id'],
                'Имя пользователя': row['first_name'] or 'Неизвестно',
                'Муниципалитет': row['city'] or 'Не указан',
                'Сумма': row['amount'],
                'Причина': row['reason'] or '',
                'ID администратора': row['admin_id'],
                'Администратор': row['admin_name'] or 'Система'
            })

        df = pd.DataFrame(data)

        # Добавляем итоговую статистику
        summary = df.groupby('Муниципалитет').agg({
            'Сумма': 'sum',
            'ID пользователя': 'nunique'
        }).reset_index()

        summary.columns = ['Муниципалитет', 'Всего баллов', 'Уникальных пользователей']

        # Создаем файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'points_history_{timestamp}.xlsx'

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='История операций', index=False)
            summary.to_excel(writer, sheet_name='Итоги по муниципалитетам', index=False)

            # Форматируем
            workbook = writer.book
            worksheet = writer.sheets['История операций']

            # Автоширина столбцов
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        return filename, None

    except Exception as e:
        return None, f"Ошибка генерации отчета: {str(e)}"
def ask_report_period(chat_id):
    """Запрос периода для отчета"""
    msg = bot.send_message(
        chat_id,
        "📅 <b>Выгрузка истории операций</b>\n\n"
        "Введите период в формате:\n"
        "<code>ДД.ММ.ГГГГ - ДД.ММ.ГГГГ</code>\n\n"
        "<i>Пример: 01.02.2024 - 29.02.2024</i>\n"
        "Или отправьте '-' для отчета за все время:",
        parse_mode='HTML'
    )

    bot.register_next_step_handler(msg, process_report_period, chat_id)
def process_report_period(message, chat_id):
    """Обработка периода отчета"""
    period_text = message.text.strip()

    if period_text == '-':
        start_date = None
        end_date = None
        period_info = "за все время"
    else:
        try:
            dates = period_text.split('-')
            if len(dates) != 2:
                raise ValueError

            start_date_str = dates[0].strip()
            end_date_str = dates[1].strip()

            start_date = datetime.strptime(start_date_str, "%d.%m.%Y").strftime("%Y-%m-%d 00:00:00")
            end_date = datetime.strptime(end_date_str, "%d.%m.%Y").strftime("%Y-%m-%d 23:59:59")

            period_info = f"с {start_date_str} по {end_date_str}"

        except ValueError:
            bot.send_message(
                chat_id,
                "❌ Неверный формат периода. Используйте: ДД.ММ.ГГГГ - ДД.ММ.ГГГГ\n"
                "Пример: 01.02.2024 - 29.02.2024"
            )
            ask_report_period(chat_id)
            return

    # Генерируем отчет
    bot.send_message(chat_id, f"⏳ Генерирую отчет {period_info}...")

    filename, error = generate_points_history_report(start_date, end_date)

    if error:
        bot.send_message(chat_id, f"❌ {error}")
        show_admin_panel(chat_id)
        return

    # Отправляем файл
    try:
        with open(filename, 'rb') as file:
            bot.send_document(
                chat_id,
                file,
                caption=f"📊 <b>Отчет по истории операций</b>\n{period_info}",
                parse_mode='HTML'
            )

        # Удаляем временный файл
        os.remove(filename)

    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка отправки файла: {str(e)}")

    # Возвращаем в админ-панель
    show_admin_panel(chat_id)
def remove_task_from_excel(task_index):
    """Удалить задачу из Excel файла"""
    try:
        # Загружаем Excel
        df = pd.read_excel(EXCEL_FILE_PATH, engine='openpyxl')

        if task_index >= len(df):
            return False, "❌ Задача не найдена"

        # Получаем данные задачи перед удалением
        task_row = df.iloc[task_index]
        task_name = task_row['Задача']
        city = task_row.get('Ответственный', 'Не указан')

        # Удаляем задачу
        df = df.drop(index=task_index).reset_index(drop=True)
        df.to_excel(EXCEL_FILE_PATH, index=False)

        return True, f"✅ Задача удалена из Excel:\n<b>{task_name}</b>\n📍 {city}"

    except Exception as e:
        return False, f"❌ Ошибка при удалении: {str(e)}"

# ==============================
# НОВЫЕ ФУНКЦИИ ДЛЯ УПРАВЛЕНИЯ ОТВЕТСТВЕННЫМИ
# ==============================
def clear_task_responsible(task_index):
    """Очистить поле 'Ответственный' в задаче из Excel"""
    try:
        # Загружаем Excel
        df = pd.read_excel(EXCEL_FILE_PATH, engine='openpyxl')

        if task_index >= len(df):
            return False, "❌ Задача не найдена"

        # Получаем данные задачи перед очисткой
        task_row = df.iloc[task_index]
        task_name = task_row['Задача']
        old_city = task_row.get('Ответственный', 'Не указан')

        # Очищаем поле Ответственный
        df.at[task_index, 'Ответственный'] = ''
        df.to_excel(EXCEL_FILE_PATH, index=False)

        return True, f"✅ Ответственный очищен:\n<b>{task_name}</b>\n📍 Было: {old_city}"

    except Exception as e:
        return False, f"❌ Ошибка при очистке ответственного: {str(e)}"
def complete_task_with_points(task_index, user_id, points=0, reason=""):
    """Отметить задачу как выполненную и обновить счётчики пользователя"""
    try:
        # Загружаем Excel
        df = pd.read_excel(EXCEL_FILE_PATH, engine='openpyxl')

        if task_index >= len(df):
            return False, "❌ Задача не найдена"

        task_row = df.iloc[task_index]
        task_name = task_row['Задача']
        responsible_city = task_row.get('Ответственный', '')

        # Проверяем, назначена ли задача пользователю
        user = get_user_info(user_id)
        if not user:
            return False, "❌ Пользователь не найден"

        user_city = user['city']

        # Проверяем, соответствует ли ответственный муниципалитету пользователя
        if responsible_city != user_city and responsible_city != "Все муниципалитеты":
            return False, f"❌ Эта задача назначена на {responsible_city}, а не на ваш муниципалитет ({user_city})"

        # Удаляем задачу из Excel (или помечаем как выполненную)
        # Вариант 1: Удаляем задачу
        df = df.drop(index=task_index).reset_index(drop=True)
        df.to_excel(EXCEL_FILE_PATH, index=False)

        # Обновляем счётчик выполненных задач пользователя
        new_counter_value = update_user_counter(user_id, 'completed_tasks', 1)

        # Начисляем баллы если указаны
        if points > 0:
            new_points = update_user_points(user_id, points)
            log_points_history(user_id, points, f"Выполнение задачи: {task_name} ({reason})", user_id)

        return True, f"✅ Задача выполнена!\n📊 Выполнено ТЗ: {new_counter_value}\n{'🏅 +' + str(points) + ' баллов' if points > 0 else ''}"

    except Exception as e:
        return False, f"❌ Ошибка: {str(e)}"

# ==============================
# 4. ФУНКЦИИ ПОЛЬЗОВАТЕЛЬСКОГО ИНТЕРФЕЙСА
# ==============================
def show_personal_cabinet(user_id, chat_id):
    user = get_user_info(user_id)
    if not user:
        bot.send_message(chat_id, "❌ Пользователь не найден")
        return

    # Обновляем активность
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET last_active = ? WHERE user_id = ?',
                   (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user_id))
    conn.commit()

    # Получаем счётчики
    counters = get_user_counters(user_id)

    # Получаем достижения
    cursor.execute('''
        SELECT achievement_id, unlocked_at 
        FROM user_achievements 
        WHERE user_id = ? 
        ORDER BY unlocked_at DESC
    ''', (user_id,))
    achievements = cursor.fetchall()

    # Формируем строку счётчиков
    counters_text = ""
    for counter_type, config in COUNTERS_CONFIG.items():
        value = counters.get(counter_type, 0)
        # Добавляем эмодзи для каждого типа
        if counter_type == 'completed_tasks':
            counters_text += f"• ✅ {config['name']}: <b>{value}</b>\n"
        elif counter_type == 'content_ideas':
            counters_text += f"• 💡 {config['name']}: <b>{value}</b>\n"
        elif counter_type == 'meetings_attended':
            counters_text += f"• 📅 {config['name']}: <b>{value}</b>\n"
        elif counter_type == 'raspush_completed':
            counters_text += f"• 🚀 {config['name']}: <b>{value}</b>\n"
        else:
            counters_text += f"• {config['name']}: <b>{value}</b>\n"

    # Формируем строку достижений
    achievements_text = ""
    if achievements:
        for ach in achievements[:5]:
            emoji = ACHIEVEMENT_EMOJIS.get(ach['achievement_id'], '🏆')
            achievements_text += f"{emoji} "
    else:
        achievements_text = "🎯 Достижений пока нет"

    city_emoji = AVAILABLE_CITIES.get(user['city'], '🏙️')
    response = (
        f"<b>👤 Личный кабинет</b>\n\n"
        f"<b>Имя:</b> {user['first_name']}\n"
        f"<b>Муниципалитет:</b> {city_emoji} {user['city']}\n"
        f"<b>Баллы:</b> 🏅 <b>{user['points']}</b>\n\n"
        f"<b>📊 Ваши показатели:</b>\n{counters_text}\n"
        f"<b>🏆 Достижения:</b>\n{achievements_text}\n\n"
        f"<i>Изменить муниципалитет: /setcity</i>"
    )

    markup = types.InlineKeyboardMarkup(row_width=2)

    # Основные кнопки
    main_buttons = [
        types.InlineKeyboardButton('⚡ Мои задачи', callback_data='my_city_tasks'),
        types.InlineKeyboardButton('📜 Список задач', callback_data='all_tasks_list'),
        types.InlineKeyboardButton('📈 История операций', callback_data='user_history'),
        types.InlineKeyboardButton('🏆 Все достижения', callback_data='show_all_achievements'),
        types.InlineKeyboardButton('📊 Рейтинг муниципалитетов', callback_data='city_rating'),
        types.InlineKeyboardButton('📋 Справка ', callback_data='show_rules'),
        types.InlineKeyboardButton('📍 Изменить муниципалитет', callback_data='change_city'),
        types.InlineKeyboardButton('📅 Контент-план', callback_data='show_content_plan'),
    ]

    # Добавляем по две кнопки в ряд
    for i in range(0, len(main_buttons), 2):
        if i + 1 < len(main_buttons):
            markup.add(main_buttons[i], main_buttons[i + 1])
        else:
            markup.add(main_buttons[i])

    # Кнопка для администраторов
    if is_admin(user_id):
        markup.add(types.InlineKeyboardButton('⚙️ Админ-панель', callback_data='admin_panel'))

    bot.send_message(chat_id, response, parse_mode='HTML', reply_markup=markup)
def show_city_selection(user_id, chat_id, page=0):
    """Выбор муниципалитета при регистрации"""
    cities_list = list(AVAILABLE_CITIES.items())
    cities_per_page = 6
    total_pages = (len(cities_list) + cities_per_page - 1) // cities_per_page

    start_idx = page * cities_per_page
    end_idx = start_idx + cities_per_page
    current_cities = cities_list[start_idx:end_idx]

    markup = types.InlineKeyboardMarkup(row_width=2)
    for city, emoji in current_cities:
        markup.add(types.InlineKeyboardButton(f"{emoji} {city}", callback_data=f'select_city_{city}'))

    # Навигация
    navigation = []
    if page > 0:
        navigation.append(types.InlineKeyboardButton('⬅️ Назад', callback_data=f'city_page_{page - 1}'))
    if page < total_pages - 1:
        navigation.append(types.InlineKeyboardButton('Далее ➡️', callback_data=f'city_page_{page + 1}'))

    if navigation:
        markup.add(*navigation)

    bot.send_message(
        chat_id,
        f"🏙️ <b>Выберите муниципалитет:</b>\nСтраница {page + 1}/{total_pages}",
        parse_mode='HTML',
        reply_markup=markup
    )

# ==============================
# 5. ФУНКЦИИ АДМИНИСТРИРОВАНИЯ
# ==============================
def show_admin_panel(chat_id):
    """Админ-панель"""
    if not is_admin(chat_id):
        bot.send_message(chat_id, "⛔ Нет доступа")
        return

    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton('📊 Список пользователей', callback_data='admin_list_users'),
        types.InlineKeyboardButton('➕ Начислить баллы', callback_data='admin_add_points_menu'),
        types.InlineKeyboardButton('➖ Снять баллы', callback_data='admin_remove_points_menu'),
        types.InlineKeyboardButton('🌐 Изменить муниципалитет', callback_data='admin_change_city'),
        types.InlineKeyboardButton('📋 Установить правила', callback_data='admin_set_rules'),
        types.InlineKeyboardButton('📅 Обновить контент-план', callback_data='admin_set_content_plan'),
        types.InlineKeyboardButton('⭐️ Достижения', callback_data='admin_achievements'),
        types.InlineKeyboardButton('🏆 Рейтинг муниципалитетов', callback_data='admin_city_stats'),
        types.InlineKeyboardButton('📋 Задачи муниципалитетам', callback_data='admin_city_tasks'),
        types.InlineKeyboardButton('📨 Сделать рассылку', callback_data='admin_broadcast'),
        types.InlineKeyboardButton('📈 История операций', callback_data='admin_history_report'),
        types.InlineKeyboardButton('🚪 Выйти', callback_data='exit_admin')
    ]

    # Добавляем кнопки парами
    for i in range(0, len(buttons), 2):
        if i + 1 < len(buttons):
            markup.add(buttons[i], buttons[i + 1])
        else:
            markup.add(buttons[i])

    bot.send_message(
        chat_id,
        "<b>⚙️ Админ-панель</b>\n\n"
        "<b>Управление контентом:</b>\n"
        "• 📋 Установить правила работы\n"
        "• 📅 Обновить контент-план\n\n"
        "<b>Управление пользователями:</b>\n"
        "• 📊 Просмотр списка\n"
        "• ➕/➖ Начисление/снятие баллов\n"
        "• 📍 Изменение муниципалитета\n"
        "• 🏆 Управление достижениями\n\n"
        "<b>Рассылки:</b>\n"
        "• 📨 Отправка сообщений",
        parse_mode='HTML',
        reply_markup=markup
    )
def show_achievements_admin_panel(chat_id):
    """Панель управления достижениями для администратора"""
    markup = types.InlineKeyboardMarkup(row_width=2)

    buttons = [
        types.InlineKeyboardButton('➕ Добавить ТЗ', callback_data='admin_add_task'),
        types.InlineKeyboardButton('💡 Добавить идею', callback_data='admin_add_idea'),
        types.InlineKeyboardButton('📋 Добавить планёрку', callback_data='admin_add_meeting'),
        types.InlineKeyboardButton('🏆 Выдать достижение', callback_data='admin_give_achievement'),
        types.InlineKeyboardButton('🗑️ Снять достижение', callback_data='admin_remove_achievement'),
        types.InlineKeyboardButton('📊 Статистика планёрок', callback_data='admin_meetings_stats'),
        types.InlineKeyboardButton('📈 Общая статистика', callback_data='admin_achievements_stats'),
        types.InlineKeyboardButton('👤 Достижения пользователя', callback_data='admin_view_user_achievements'),
        types.InlineKeyboardButton('🔙 Назад', callback_data='admin_panel')
    ]

    # Добавляем кнопки парами
    for i in range(0, len(buttons), 2):
        if i + 1 < len(buttons):
            markup.add(buttons[i], buttons[i + 1])
        else:
            markup.add(buttons[i])

    bot.send_message(
        chat_id,
        "<b>🏆 Управление достижениями и планёрками</b>\n\n"
        "<b>Счётчики:</b>\n"
        "• ➕ Добавить выполненное ТЗ\n"
        "• 💡 Добавить идею для контент-плана\n"
        "• 📋 Добавить участие в планёрке\n\n"
        "<b>Достижения:</b>\n"
        "• 🏆 Выдать специальное достижение\n\n"
        "<b>Статистика:</b>\n"
        "• 📊 Посещение планёрок\n"
        "• 📈 Общая статистика\n"
        "• 👤 Достижения конкретного пользователя",
        parse_mode='HTML',
        reply_markup=markup
    )

# Функции для работы с баллами
def show_user_selection_for_points(chat_id, action='add'):
    """Выбор пользователя для начисления/снятия баллов"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, first_name, city, points FROM users ORDER BY points DESC LIMIT 15')
    users = cursor.fetchall()

    markup = types.InlineKeyboardMarkup(row_width=2)

    # Топ-15 пользователей
    for user in users:
        city_emoji = AVAILABLE_CITIES.get(user['city'], '🏙️')
        button_text = f"{user['first_name']} ({city_emoji} {user['city']}) - {user['points']} баллов"
        markup.add(types.InlineKeyboardButton(button_text, callback_data=f'select_user_{action}_{user["user_id"]}'))

    # Кнопка для ручного ввода ID
    markup.add(types.InlineKeyboardButton('✏️ Ввести ID вручную', callback_data=f'manual_id_{action}'))
    markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='admin_panel'))

    action_text = "начисления" if action == 'add' else "снятия"
    bot.send_message(chat_id, f"👥 <b>Выберите пользователя для {action_text}:</b>\n\n"
                              f"<i>Выберите из списка или введите ID вручную</i>",
                     parse_mode='HTML', reply_markup=markup)


def process_manual_id(message, action, original_chat_id):
    """Обработка ручного ввода ID пользователя"""
    try:
        target_user_id = int(message.text.strip())

        # Проверяем существование пользователя
        user = get_user_info(target_user_id)
        if not user:
            bot.send_message(
                message.chat.id,
                f"❌ Пользователь с ID {target_user_id} не найден",
                parse_mode='HTML'
            )
            show_user_selection_for_points(original_chat_id, action)
            return

        # Переходим к выбору количества баллов
        show_points_amount_selection(original_chat_id, target_user_id, action)

    except ValueError:
        bot.send_message(
            message.chat.id,
            "❌ ID должен быть числом",
            parse_mode='HTML'
        )
        show_user_selection_for_points(original_chat_id, action)
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Ошибка: {str(e)}")
        show_user_selection_for_points(original_chat_id, action)

def show_points_amount_selection(chat_id, user_id, action='add'):
    """Выбор количества баллов"""
    markup = types.InlineKeyboardMarkup(row_width=3)
    points_options = [1, 2, 3, 5, 10, 15, 20]

    buttons = []
    for points in points_options:
        sign = '+' if action == 'add' else '-'
        buttons.append(types.InlineKeyboardButton(f"{sign}{points}",
                                                  callback_data=f'select_points_{action}_{user_id}_{points}'))

    # Разбиваем на строки по 3 кнопки
    for i in range(0, len(buttons), 3):
        markup.add(*buttons[i:i + 3])

    markup.add(types.InlineKeyboardButton('✏️ Своё количество', callback_data=f'custom_points_{action}_{user_id}'))
    markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data=f'admin_{action}_points_menu'))

    action_text = "начислить" if action == 'add' else "снять"
    bot.send_message(chat_id, f"💰 <b>Выберите количество баллов для {action_text}:</b>",
                     parse_mode='HTML', reply_markup=markup)
def ask_for_reason(chat_id, user_id, points, action='add'):
    """Запрос причины"""
    action_text = "начисления" if action == 'add' else "снятия"
    sign = '+' if action == 'add' else '-'

    msg = bot.send_message(
        chat_id,
        f"📝 <b>Введите причину {action_text} баллов:</b>\n"
        f"Количество: {sign}{points} баллов",
        parse_mode='HTML'
    )

    bot.register_next_step_handler(msg, process_reason_input, user_id, points, action, chat_id)
def process_reason_input(message, target_user_id, points, action, original_chat_id):
    """Обработка причины"""
    reason = message.text.strip()
    if not reason:
        bot.send_message(original_chat_id, "❌ Причина не может быть пустой")
        return

    execute_points_operation(
        chat_id=original_chat_id,
        target_user_id=target_user_id,
        points=points,
        reason=reason,
        action=action,
        admin_id=message.from_user.id
    )
def execute_points_operation(chat_id, target_user_id, points, reason, action, admin_id):
    """Выполнение операции с баллами"""
    try:
        user = get_user_info(target_user_id)
        if not user:
            bot.send_message(chat_id, "❌ Пользователь не найден")
            return

        points_amount = points if action == 'add' else -points

        # Проверка для снятия баллов
        if action == 'remove' and user['points'] < points:
            points = user['points']
            points_amount = -points
            bot.send_message(chat_id, f"⚠️ Будет списано {points} баллов")

        new_points = update_user_points(target_user_id, points_amount)
        log_points_history(target_user_id, points_amount, reason, admin_id)

        # Уведомление пользователя
        try:
            city_emoji = AVAILABLE_CITIES.get(user['city'], '🏙️')
            action_text = "начислены" if action == 'add' else "списаны"
            sign = "+" if action == 'add' else "-"

            bot.send_message(target_user_id,
                             f"🎉 <b>Вам {action_text} баллы!</b>\n\n"
                             f"Количество: {sign}{points}\n"
                             f"Причина: {reason}\n"
                             f"Всего баллов: {new_points}",
                             parse_mode='HTML')
        except:
            pass

        city_emoji = AVAILABLE_CITIES.get(user['city'], '🏙️')
        action_text = "начислено" if action == 'add' else "списано"

        bot.send_message(chat_id,
                         f"✅ {action_text} {points} баллов пользователю {user['first_name']}\n"
                         f"📍 {city_emoji} {user['city']}\n"
                         f"Причина: {reason}\n"
                         f"Теперь: {new_points} баллов")

    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")
def show_user_history(user_id, chat_id, message_id=None):
    """Показать историю операций пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT date, amount, reason, admin_id
        FROM points_history
        WHERE user_id = ?
        ORDER BY date DESC
        LIMIT 10
    ''', (user_id,))

    history = cursor.fetchall()

    user = get_user_info(user_id)
    if not user:
        bot.send_message(chat_id, "❌ Пользователь не найден")
        return

    response = f"📈 <b>Ваша история операций</b>\n\n"
    response += f"<b>Текущий баланс:</b> 🏅 {user['points']}\n\n"

    if history:
        response += "<b>Последние 10 операций:</b>\n\n"
        for i, record in enumerate(history, 1):
            sign = "+" if record['amount'] > 0 else ""
            date_str = datetime.strptime(record['date'], "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y %H:%M")
            response += f"{i}. {date_str}\n"
            response += f"   <b>{sign}{record['amount']}</b> баллов\n"
            response += f"   Причина: {record['reason'] or 'не указана'}\n"
            response += f"{'-' * 30}\n"
    else:
        response += "У вас пока нет истории операций.\n"

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='personal_cabinet'))

    if message_id:
        bot.edit_message_text(response, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
    else:
        bot.send_message(chat_id, response, parse_mode='HTML', reply_markup=markup)

# Функции рассылки
def show_broadcast_options(chat_id):
    """Опции рассылки"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton('📢 Всем пользователям', callback_data='broadcast_all'),
        types.InlineKeyboardButton('🏙️ Муниципалитету', callback_data='broadcast_by_city'),
        types.InlineKeyboardButton('🔙 Назад', callback_data='admin_panel')
    ]

    markup.add(*buttons)
    bot.send_message(chat_id, "<b>📨 Выберите тип рассылки:</b>", parse_mode='HTML', reply_markup=markup)
def show_cities_for_broadcast(chat_id):
    """Выбор муниципалитета для рассылки"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT city FROM users WHERE city != "Не указан" ORDER BY city')
    cities = cursor.fetchall()

    markup = types.InlineKeyboardMarkup(row_width=2)
    for city_record in cities:
        city = city_record['city']
        city_emoji = AVAILABLE_CITIES.get(city, '🏙️')
        markup.add(types.InlineKeyboardButton(f"{city_emoji} {city}", callback_data=f'broadcast_city_{city}'))

    markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='admin_broadcast'))
    bot.send_message(chat_id, "<b>🏙️ Выберите муниципалитет:</b>", parse_mode='HTML', reply_markup=markup)
def ask_for_broadcast_text(chat_id, target_type, target_value):
    """Запрос текста рассылки"""
    if target_type == 'all':
        target_info = "📢 <b>Всем пользователям</b>"
    elif target_type == 'city':
        city_emoji = AVAILABLE_CITIES.get(target_value, '🏙️')
        target_info = f"🏙️ <b>Муниципалитету:</b> {city_emoji} {target_value}"

    msg = bot.send_message(
        chat_id,
        f"<b>✏️ Введите текст рассылки:</b>\n\n{target_info}",
        parse_mode='HTML'
    )

    bot.register_next_step_handler(msg, process_broadcast_text, target_type, target_value, chat_id)
def process_broadcast_text(message, target_type, target_value, original_chat_id):
    """Обработка текста рассылки"""
    broadcast_text = message.text.strip()

    if not broadcast_text:
        bot.send_message(original_chat_id, "❌ Текст не может быть пустым")
        return

    # Автоматически определяем форматирование
    if "```" in broadcast_text or "`" in broadcast_text:
        parse_mode = 'MarkdownV2'
    elif "<" in broadcast_text and ">" in broadcast_text:
        parse_mode = 'HTML'
    else:
        parse_mode = None

    # Сохраняем текст в кэше
    cache_key = f"{original_chat_id}_{target_type}_{target_value}"
    broadcast_cache[cache_key] = {
        'text': broadcast_text,
        'parse_mode': parse_mode
    }

    # Показываем предпросмотр
    try:
        if parse_mode == 'HTML':
            preview_text = f"""
<b>📋 Предпросмотр рассылки (HTML):</b>

{broadcast_text}

────────────────
<i>Отправить это сообщение?</i>
"""
        elif parse_mode == 'MarkdownV2':
            preview_text = f"""
*📋 Предпросмотр рассылки (Markdown):*

{broadcast_text}

────────────────
_Отправить это сообщение?_
"""
        else:
            preview_text = f"""
📋 Предпросмотр рассылки:

{broadcast_text}

────────────────
Отправить это сообщение?
"""

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton('✅ Отправить', callback_data=f'confirm_broadcast_{cache_key}'),
            types.InlineKeyboardButton('❌ Отмена', callback_data='admin_broadcast')
        )

        bot.send_message(original_chat_id, preview_text,
                         parse_mode='HTML' if parse_mode != 'MarkdownV2' else 'MarkdownV2',
                         reply_markup=markup)

    except Exception as e:
        # Если форматирование не сработало, отправляем как обычный текст
        bot.send_message(original_chat_id,
                         f"⚠️ <b>Ошибка форматирования:</b>\n\n"
                         f"Отправлю как обычный текст.\n\n"
                         f"{broadcast_text}",
                         parse_mode='HTML',
                         reply_markup=markup)
def send_broadcast(chat_id, target_type, target_value, broadcast_data, admin_id):
    """Отправить рассылку с учетом форматирования"""
    try:
        broadcast_text = broadcast_data['text']
        parse_mode = broadcast_data.get('parse_mode')

        conn = get_db_connection()
        cursor = conn.cursor()

        if target_type == 'all':
            cursor.execute('SELECT user_id FROM users WHERE is_banned = 0')
            target_description = "всем пользователям"
        elif target_type == 'city':
            cursor.execute('SELECT user_id FROM users WHERE city = ? AND is_banned = 0', (target_value,))
            city_emoji = AVAILABLE_CITIES.get(target_value, '🏙️')
            target_description = f"муниципалитету {city_emoji} {target_value}"

        recipients = cursor.fetchall()

        successful, failed = 0, 0

        for recipient in recipients:
            try:
                if parse_mode:
                    bot.send_message(recipient['user_id'], broadcast_text, parse_mode=parse_mode)
                else:
                    bot.send_message(recipient['user_id'], broadcast_text)
                successful += 1
            except:
                failed += 1

        # Отчет
        report = f"""
📊 <b>Отчет о рассылке:</b>

<blockquote>Цель: {target_description}
Всего получателей: {len(recipients)}
✅ Успешно отправлено: {successful}
❌ Не удалось отправить: {failed}</blockquote>

<i>{"Некоторые пользователи могли заблокировать бота" if failed > 0 else "Все сообщения доставлены"}</i>
"""

        bot.send_message(chat_id, report, parse_mode='HTML')

    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")


# ======================================
# РАСПУШ - ИСПРАВЛЕННАЯ ВЕРСИЯ
# ======================================

# Единый словарь для хранения активных задач распуша
raspush_active_tasks = {}  # {user_id: task_id}

admin_raspush_creation = {}

def create_raspush_task(task_name, task_description):
    """Создать новую задачу РАСПУШ и разослать уведомления"""
    conn = get_db_connection()
    cursor = conn.cursor()

    now = datetime.now()
    expires = now + timedelta(days=7)

    cursor.execute('''
        INSERT INTO raspush_tasks (task_name, task_description, created_at, expires_at)
        VALUES (?, ?, ?, ?)
    ''', (
        task_name,
        task_description,
        now.strftime("%Y-%m-%d %H:%M:%S"),
        expires.strftime("%Y-%m-%d %H:%M:%S")
    ))

    task_id = cursor.lastrowid
    conn.commit()

    # Уведомляем всех пользователей
    notify_all_about_raspush(task_id, task_name, task_description)

    return task_id
def notify_all_about_raspush(task_id, name, description):
    """Отправить уведомление о новой задаче распуша всем пользователям"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT user_id FROM users WHERE is_banned = 0')
    users = cursor.fetchall()

    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            "✅ Отметить выполненной",
            callback_data=f"raspush_start_{task_id}"
        )
    )

    for user in users:
        try:
            bot.send_message(
                user['user_id'],
                f"🚀 <b>НОВАЯ ЗАДАЧА РАСПУШ</b>\n\n"
                f"<b>{name}</b>\n\n"
                f"{description}\n\n"
                f"<i>За задачу начисляются баллы!</i>",
                parse_mode='HTML',
                reply_markup=markup
            )
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("raspush_start_"))
def handle_raspush_start(call):
    """Обработчик кнопки начала выполнения распуша"""
    user_id = call.from_user.id

    # Получаем ID задачи
    task_id = int(call.data.split("_")[-1])

    # Проверяем, есть ли у пользователя муниципалитет
    user = get_user_info(user_id)
    if not user or user['city'] == 'Не указан':
        bot.answer_callback_query(call.id, "❌ Сначала выберите муниципалитет в /setcity")
        return

    # Проверяем, не выполнял ли уже этот муниципалитет задачу
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT 1 FROM raspush_completions 
        WHERE task_id = ? AND city = ?
    ''', (task_id, user['city']))

    if cursor.fetchone():
        bot.answer_callback_query(call.id, "❌ Ваш муниципалитет уже выполнил эту задачу")
        return

    # Сохраняем в активные задачи
    raspush_active_tasks[user_id] = task_id

    # Убираем кнопку из сообщения
    bot.edit_message_reply_markup(
        call.message.chat.id,
        call.message.message_id,
        reply_markup=None
    )

    bot.send_message(
        user_id,
        "📎 <b>Отправьте ссылки на посты</b>\n\n"
        "Допустимые форматы:\n"
        "• https://vk.com/...\n"
        "• https://t.me/...\n\n"
        "<i>Если отправите ссылки и на VK, и на Telegram - получите 2 балла!</i>",
        parse_mode='HTML'
    )

    bot.answer_callback_query(call.id)

@bot.message_handler(func=lambda message: message.from_user.id in raspush_active_tasks)
def handle_raspush_links_submission(message):
    """Обработчик отправки ссылок для распуша"""
    user_id = message.from_user.id
    task_id = raspush_active_tasks[user_id]
    text = message.text.strip()

    # Находим все ссылки
    links = re.findall(r'https?://[^\s]+', text)

    vk_links = []
    tg_links = []

    for link in links:
        if "vk.com" in link or "vk.ru" in link:
            vk_links.append(link)
        elif "t.me" in link:
            tg_links.append(link)

    # Убираем дубликаты
    vk_links = list(set(vk_links))
    tg_links = list(set(tg_links))

    # Проверяем, есть ли хоть одна допустимая ссылка
    total_valid_links = len(vk_links) + len(tg_links)
    if total_valid_links == 0:
        bot.send_message(
            user_id,
            "❌ Не найдено допустимых ссылок.\n\n"
            "Поддерживаются VK и Telegram ссылки.",
            parse_mode='HTML'
        )
        return

    # Определяем количество баллов
    platforms_count = 0
    if vk_links:
        platforms_count += 1
    if tg_links:
        platforms_count += 1

    points = 1 if platforms_count == 1 else 2

    # Получаем информацию о пользователе
    user = get_user_info(user_id)

    # Сохраняем выполнение
    conn = get_db_connection()
    cursor = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    all_links = "\n".join(vk_links + tg_links)

    try:
        cursor.execute('''
            INSERT INTO raspush_completions 
            (task_id, user_id, city, links, completed_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (task_id, user_id, user['city'], all_links, now))
        conn.commit()
    except sqlite3.IntegrityError:
        # Уже есть запись для этого города
        bot.send_message(
            user_id,
            "❌ Ваш муниципалитет уже выполнил эту задачу.",
            parse_mode='HTML'
        )
        del raspush_active_tasks[user_id]
        return

    # Обновляем счётчик выполненных распушей
    new_raspush_count = update_user_counter(user_id, 'raspush_completed', 1)

    # И в сообщении пользователю:
    f"📋 Выполнено распушей: {new_raspush_count}\n\n"

    # Начисляем баллы
    new_points = update_user_points(user_id, points)
    log_points_history(user_id, points, f"Распуш-задача #{task_id}", user_id)

    # Сохраняем в Excel для отчета
    save_raspush_to_excel(user['city'], all_links, task_id)

    # Отправляем подтверждение
    bot.send_message(
        user_id,
        f"✅ <b>Задача выполнена!</b>\n\n"
        f"📊 VK ссылок: {len(vk_links)}\n"
        f"📊 Telegram ссылок: {len(tg_links)}\n"
        f"🏅 Начислено баллов: +{points}\n"
        f"💰 Текущий баланс: {new_points}\n\n"
        f"<i>Спасибо за работу!</i>",
        parse_mode='HTML'
    )

    # Удаляем из активных задач
    del raspush_active_tasks[user_id]

    # Уведомляем админов о выполнении
    for admin_id in ADMIN_IDS:
        try:
            city_emoji = AVAILABLE_CITIES.get(user['city'], '🏙️')
            bot.send_message(
                admin_id,
                f"📊 <b>Выполнен распуш #{task_id}</b>\n\n"
                f"🏙️ {city_emoji} {user['city']}\n"
                f"👤 {user['first_name']}\n"
                f"🔗 Отправил(а) ссылки\n"
                f"🏅 +{points} баллов",
                parse_mode='HTML'
            )
        except:
            pass

def save_raspush_to_excel(city, links, task_id):
    """Сохранить отчет о распуше в Excel"""
    try:
        file_name = "raspush_results.xlsx"

        if os.path.exists(file_name):
            df = pd.read_excel(file_name)
        else:
            df = pd.DataFrame(columns=["Дата", "Задача #", "Муниципалитет", "Ссылки"])

        new_row = {
            "Дата": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "Задача #": task_id,
            "Муниципалитет": city,
            "Ссылки": links
        }

        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_excel(file_name, index=False)
        return True
    except Exception as e:
        print(f"Ошибка сохранения в Excel: {e}")
        return False
def generate_raspush_report(task_id):
    """Сгенерировать отчет по задаче распуша"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT city, links, completed_at, user_id
        FROM raspush_completions 
        WHERE task_id = ?
        ORDER BY completed_at
    ''', (task_id,))

    data = cursor.fetchall()

    if not data:
        return None, "Нет данных по этой задаче"

    # Создаем DataFrame
    rows = []
    for row in data:
        rows.append({
            'Муниципалитет': row['city'],
            'Ссылки': row['links'],
            'Дата выполнения': row['completed_at'],
            'ID пользователя': row['user_id']
        })

    df = pd.DataFrame(rows)

    filename = f"raspush_report_{task_id}_{datetime.now().strftime('%Y%m%d')}.xlsx"
    df.to_excel(filename, index=False)

    return filename, None
def cleanup_old_raspush():
    """Удалить просроченные задачи распуша"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute('''
            SELECT id FROM raspush_tasks WHERE expires_at <= ?
        ''', (now,))

        expired = cursor.fetchall()

        for task in expired:
            task_id = task['id']
            cursor.execute('DELETE FROM raspush_completions WHERE task_id = ?', (task_id,))
            cursor.execute('DELETE FROM raspush_tasks WHERE id = ?', (task_id,))

        conn.commit()
    except Exception as e:
        print(f"Ошибка при очистке распуша: {e}")

@bot.callback_query_handler(func=lambda call: call.data == "admin_create_raspush")
def admin_create_raspush_handler(call):
    """Админ: начало создания задачи распуша"""
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "⛔ Нет доступа")
        return

    bot.edit_message_text(
        "🚀 <b>Создание задачи РАСПУШ</b>\n\n"
        "Введите название задачи:",
        call.message.chat.id,
        call.message.message_id,
        parse_mode="HTML"
    )

    bot.register_next_step_handler(call.message, process_raspush_name)

def process_raspush_name(message):
    """Обработка названия задачи распуша"""
    admin_id = message.from_user.id

    if not is_admin(admin_id):
        return

    admin_raspush_creation[admin_id] = {
        "name": message.text.strip()
    }

    msg = bot.send_message(
        message.chat.id,
        "✏️ <b>Введите описание задачи:</b>\n\n"
        "<i>Опишите, что нужно сделать, какие ссылки прислать</i>",
        parse_mode="HTML"
    )

    bot.register_next_step_handler(msg, process_raspush_description)
def process_raspush_description(message):
    """Обработка описания и создание задачи"""
    admin_id = message.from_user.id

    if not is_admin(admin_id):
        return

    description = message.text.strip()
    task_data = admin_raspush_creation.get(admin_id)

    if not task_data:
        bot.send_message(message.chat.id, "❌ Ошибка создания задачи")
        return

    task_name = task_data["name"]

    # Создаем задачу
    task_id = create_raspush_task(task_name, description)

    bot.send_message(
        message.chat.id,
        f"✅ <b>Задача РАСПУШ #{task_id} создана!</b>\n\n"
        f"<b>Название:</b> {task_name}\n"
        f"<b>Описание:</b> {description}\n\n"
        f"<i>Уведомления отправлены всем муниципалитетам</i>",
        parse_mode="HTML"
    )

    del admin_raspush_creation[admin_id]

    # Возвращаем в админ-панель
    show_city_admin_tasks(message.chat.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_raspush_report")
def admin_raspush_report_handler(call):
    """Админ: запрос отчета по распушу"""
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "⛔ Нет доступа")
        return

    msg = bot.send_message(
        call.message.chat.id,
        "📊 <b>Введите номер задачи РАСПУШ для отчета:</b>\n\n"
        "<i>Например: 1</i>",
        parse_mode='HTML'
    )

    bot.register_next_step_handler(msg, process_raspush_report_request)

def process_raspush_report_request(message):
    """Обработка запроса отчета по распушу"""
    try:
        task_id = int(message.text.strip())

        bot.send_message(message.chat.id, "⏳ Генерирую отчет...")

        filename, error = generate_raspush_report(task_id)

        if error:
            bot.send_message(message.chat.id, f"❌ {error}")
        else:
            with open(filename, 'rb') as file:
                bot.send_document(
                    message.chat.id,
                    file,
                    caption=f"📊 Отчет по задаче РАСПУШ #{task_id}",
                    parse_mode='HTML'
                )
            os.remove(filename)

    except ValueError:
        bot.send_message(message.chat.id, "❌ Введите номер задачи (число)")
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Ошибка: {str(e)}")

    # Возвращаем в админ-панель
    show_city_admin_tasks(message.chat.id)


# ДОБАВИТЬ эту функцию:
def delete_raspush_task(task_id, admin_id):
    """Удалить задачу распуша (только для админа)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем информацию о задаче
        cursor.execute('SELECT task_name FROM raspush_tasks WHERE id = ?', (task_id,))
        task = cursor.fetchone()

        if not task:
            return False, "Задача не найдена"

        task_name = task['task_name']

        # Удаляем выполнения
        cursor.execute('DELETE FROM raspush_completions WHERE task_id = ?', (task_id,))
        completions_count = cursor.rowcount

        # Удаляем задачу
        cursor.execute('DELETE FROM raspush_tasks WHERE id = ?', (task_id,))

        conn.commit()

        return True, f"✅ Задача '{task_name}' удалена. Выполнений: {completions_count}"

    except Exception as e:
        return False, f"Ошибка при удалении: {str(e)}"
def raspush_cleanup_scheduler():
    """Планировщик очистки просроченных задач распуша"""
    while True:
        try:
            cleanup_old_raspush()
            time.sleep(86400)  # 24 часа
        except Exception as e:
            print(f"Ошибка в планировщике распуша: {e}")
            time.sleep(3600)

# Запуск очистки просроченных задач в отдельном потоке
raspush_cleanup_thread = threading.Thread(target=raspush_cleanup_scheduler, daemon=True)
raspush_cleanup_thread.start()

# ======================================
# ДОСТИЖЕНИЯ И ПЛАНЁРКИ
# ======================================
def get_user_counters(user_id):
    """Получить все счётчики пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT counter_type, value 
        FROM user_counters 
        WHERE user_id = ?
    ''', (user_id,))

    counters = {}
    for row in cursor.fetchall():
        counters[row['counter_type']] = row['value']

    return counters
def update_user_counter(user_id, counter_type, amount=1):
    """Обновить счётчик пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Обновляем или создаём счётчик
    cursor.execute('''
        INSERT OR REPLACE INTO user_counters (user_id, counter_type, value, last_updated)
        VALUES (?, ?, COALESCE((SELECT value FROM user_counters WHERE user_id = ? AND counter_type = ?), 0) + ?, ?)
    ''', (user_id, counter_type, user_id, counter_type, amount, now))

    # Получаем новое значение
    cursor.execute('''
        SELECT value FROM user_counters WHERE user_id = ? AND counter_type = ?
    ''', (user_id, counter_type))

    new_value = cursor.fetchone()['value']

    conn.commit()

    # ============ ИЗМЕНЁННЫЙ КОД ============
    # Проверяем достижения ТОЛЬКО если они настроены
    if counter_type in COUNTERS_CONFIG:
        achievements_config = COUNTERS_CONFIG[counter_type].get('achievements', {})
        # Больше никаких автоматических проверок
    # ========================================

    return new_value
def check_achievements(user_id, counter_type, current_value):
    """Проверить и разблокировать достижения"""
    if counter_type not in COUNTERS_CONFIG:
        return

    achievements_config = COUNTERS_CONFIG[counter_type]['achievements']

    for threshold, achievement_id in achievements_config.items():
        if current_value >= threshold and not has_achievement(user_id, achievement_id):
            unlock_achievement(user_id, achievement_id)
def has_achievement(user_id, achievement_id):
    """Проверить, есть ли у пользователя достижение"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT 1 FROM user_achievements 
        WHERE user_id = ? AND achievement_id = ?
    ''', (user_id, achievement_id))

    return cursor.fetchone() is not None
def unlock_achievement(user_id, achievement_id):
    """Разблокировать достижение"""
    conn = get_db_connection()
    cursor = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Проверяем, есть ли уже это достижение
    if has_achievement(user_id, achievement_id):
        return False

    # Вставляем в user_achievements
    cursor.execute('''
        INSERT INTO user_achievements (user_id, achievement_id, unlocked_at, is_manual)
        VALUES (?, ?, ?, 0)
    ''', (user_id, achievement_id, now))

    # Вставляем в историю достижений
    cursor.execute('''
        INSERT INTO achievements_history 
        (user_id, achievement_id, unlocked_at, is_manual, points_awarded)
        VALUES (?, ?, ?, 0, ?)
    ''', (user_id, achievement_id, now, 5))

    # Начисляем баллы пользователю
    cursor.execute('UPDATE users SET points = points + 5 WHERE user_id = ?', (user_id,))

    conn.commit()

    # Логируем начисление баллов
    try:
        log_points_history(user_id, 5, f"Автоматическое достижение: {achievement_id}", None)
    except:
        pass

    # Отправляем уведомление
    notify_achievement_unlocked(user_id, achievement_id, is_manual=False)

    return True
def give_manual_achievement(user_id, achievement_id, admin_id, reason=""):
    """Выдать ручное достижение пользователю"""
    conn = get_db_connection()
    cursor = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Проверяем, есть ли уже это достижение
    cursor.execute('''
        SELECT 1 FROM user_achievements 
        WHERE user_id = ? AND achievement_id = ?
    ''', (user_id, achievement_id))

    if cursor.fetchone():
        return False, "У пользователя уже есть это достижение"

    # Добавляем достижение
    cursor.execute('''
        INSERT INTO user_achievements (user_id, achievement_id, unlocked_at, is_manual, admin_id)
        VALUES (?, ?, ?, 1, ?)
    ''', (user_id, achievement_id, now, admin_id))

    # Добавляем в историю достижений
    cursor.execute('''
        INSERT INTO achievements_history (user_id, achievement_id, unlocked_at, 
                                         is_manual, admin_id, reason, points_awarded)
        VALUES (?, ?, ?, 1, ?, ?, ?)
    ''', (user_id, achievement_id, now, admin_id, reason, 10))

    # Начисляем баллы за ручное достижение
    cursor.execute('UPDATE users SET points = points + 10 WHERE user_id = ?', (user_id,))

    conn.commit()

    # Логируем начисление баллов
    log_points_history(user_id, 10, f"Ручное достижение: {achievement_id} ({reason})", admin_id)

    # Отправляем уведомление
    notify_achievement_unlocked(user_id, achievement_id, is_manual=True)

    return True, "Достижение успешно выдано"
def remove_achievement(user_id, achievement_id, admin_id, reason=""):
    """Снять достижение у пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Проверяем, есть ли у пользователя это достижение
    cursor.execute('''
        SELECT 1 FROM user_achievements 
        WHERE user_id = ? AND achievement_id = ?
    ''', (user_id, achievement_id))

    if not cursor.fetchone():
        return False, "У пользователя нет этого достижения"

    # Удаляем достижение
    cursor.execute('''
        DELETE FROM user_achievements 
        WHERE user_id = ? AND achievement_id = ?
    ''', (user_id, achievement_id))

    # Создаем таблицу для логирования удалений, если её нет
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS removed_achievements_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            achievement_id TEXT,
            admin_id INTEGER,
            reason TEXT,
            removed_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    ''')

    # Логируем удаление
    cursor.execute('''
        INSERT INTO removed_achievements_history 
        (user_id, achievement_id, admin_id, reason, removed_at)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_id, achievement_id, admin_id, reason, now))

    conn.commit()

    return True, "Достижение успешно снято"
def notify_achievement_unlocked(user_id, achievement_id, is_manual=False):
    """Отправить уведомление о разблокированном достижении со стикером"""
    try:
        message = ACHIEVEMENT_MESSAGES.get(
            achievement_id,
            f'🎉 Поздравляем! Вы получили достижение: {achievement_id}'
        )

        user = get_user_info(user_id)
        if not user:
            return

        # 1. Отправляем стикер (если есть)
        if achievement_id in STICKER_IDS:
            sticker_id = STICKER_IDS[achievement_id]
            try:
                bot.send_sticker(user_id, sticker_id)
            except Exception as e:
                # Фоллбэк на обычный эмодзи
                emoji = ACHIEVEMENT_EMOJIS.get(achievement_id, '🏆')
                bot.send_message(user_id, emoji, parse_mode='HTML')
        else:
            # Если стикера нет, отправляем обычный эмодзи
            emoji = ACHIEVEMENT_EMOJIS.get(achievement_id, '🏆')
            bot.send_message(user_id, emoji, parse_mode='HTML')

        # 2. Отправляем текст поздравления
        bot.send_message(
            user_id,
            f"<b>🎉 Новое достижение!</b>\n\n"
            f"{message}\n\n",
            parse_mode='HTML'
        )

        # 3. Помечаем как уведомлённое
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE user_achievements 
            SET notified = 1 
            WHERE user_id = ? AND achievement_id = ?
        ''', (user_id, achievement_id))
        conn.commit()

    except Exception as e:
        print(f"Ошибка при отправке уведомления: {e}")
def show_user_achievements(user_id, chat_id, message_id=None):
    """Показать все достижения пользователя"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Получаем достижения пользователя
    cursor.execute('''
        SELECT achievement_id, unlocked_at 
        FROM user_achievements 
        WHERE user_id = ? 
        ORDER BY unlocked_at DESC
    ''', (user_id,))
    user_achievements = cursor.fetchall()

    # Получаем счётчики
    counters = get_user_counters(user_id)

    response = "<b>🏆 Ваши достижения</b>\n\n"

    # Показываем текущий прогресс
    response += "<b>📊 Прогресс:</b>\n"
    for counter_type, config in COUNTERS_CONFIG.items():
        if not config.get('achievements'):  # Пропускаем счётчики без достижений
            continue

        value = counters.get(counter_type, 0)
        counter_name = config['name']

        # Находим следующее достижение
        next_threshold = None
        for threshold, achievement_id in config['achievements'].items():
            if value < threshold:
                next_threshold = threshold
                break

        response += f"• {counter_name}: <b>{value}</b>"
        if next_threshold:
            progress = int((value / next_threshold) * 10)
            progress_bar = "█" * progress + "░" * (10 - progress)
            response += f"  [{progress_bar}] {next_threshold}\n"
        else:
            response += "  ✅ Максимум достигнут!\n"

    response += "\n<b>🎖️ Полученные достижения:</b>\n"
    if user_achievements:
        for ach in user_achievements:
            message = ACHIEVEMENT_MESSAGES.get(ach['achievement_id'], ach['achievement_id'])
            date = datetime.strptime(ach['unlocked_at'], "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y")
            response += f"{message} ({date})\n"
    else:
        response += "Пока нет достижений. Продолжайте работать!\n"

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='personal_cabinet'))

    if message_id:
        bot.edit_message_text(
            response,
            chat_id,
            message_id,
            parse_mode='HTML',
            reply_markup=markup
        )
    else:
        bot.send_message(
            chat_id,
            response,
            parse_mode='HTML',
            reply_markup=markup
        )
def show_users_for_achievement(chat_id, action):
    """Показать список пользователей для управления счётчиками"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, first_name, city, points FROM users ORDER BY points DESC LIMIT 15')
    users = cursor.fetchall()

    markup = types.InlineKeyboardMarkup(row_width=2)
    for user in users:
        city_emoji = AVAILABLE_CITIES.get(user['city'], '🏙️')
        button_text = f"{user['first_name']} ({city_emoji} {user['city']})"
        markup.add(
            types.InlineKeyboardButton(button_text, callback_data=f'achievement_user_{action}_{user["user_id"]}'))

    markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='admin_achievements'))

    bot.send_message(
        chat_id,
        f"👥 <b>Выберите пользователя:</b>",
        parse_mode='HTML',
        reply_markup=markup
    )
def get_achievement_emoji(achievement_id):
    """Получить эмодзи для достижения"""
    return ACHIEVEMENT_EMOJIS.get(achievement_id, '🏆')
def process_manual_achievement_reason(message, user_id, achievement_id, original_chat_id):
    """Обработка причины выдачи ручного достижения"""
    reason = message.text.strip()
    if reason == '-':
        reason = ""

    success, result_message = give_manual_achievement(user_id, achievement_id, message.from_user.id, reason)

    if success:
        bot.send_message(
            original_chat_id,
            f"✅ <b>Достижение выдано!</b>\n\n"
            f"<b>Достижение:</b> {ACHIEVEMENT_EMOJIS.get(achievement_id)} {achievement_id}\n"
            f"<b>Получатель ID:</b> {user_id}\n"
            f"<b>Причина:</b> {reason if reason else 'не указана'}\n\n",
            parse_mode='HTML'
        )
    else:
        bot.send_message(original_chat_id, f"❌ {result_message}")

    show_achievements_admin_panel(original_chat_id)
def show_custom_achievement_selection(chat_id):
    """Выбор достижения для выдачи"""
    markup = types.InlineKeyboardMarkup(row_width=2)

    buttons = []
    for achievement_id, emoji in ACHIEVEMENT_EMOJIS.items():
        # Показываем только ручные достижения
        if achievement_id in ['Автор MAX', 'Мастер распуша', 'ТОП февраль', 'Лайк февраль', 'Охват февраль']:
            buttons.append(
                types.InlineKeyboardButton(
                    f"{emoji} {achievement_id}",
                    callback_data=f'give_achievement_{achievement_id}'
                )
            )

    # Разбиваем на строки по 2 кнопки
    for i in range(0, len(buttons), 2):
        if i + 1 < len(buttons):
            markup.add(buttons[i], buttons[i + 1])
        else:
            markup.add(buttons[i])

    markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='admin_achievements'))

    bot.send_message(
        chat_id,
        "<b>🏆 Выберите достижение для выдачи:</b>",
        parse_mode='HTML',
        reply_markup=markup
    )
def show_remove_achievement_selection(chat_id):
    """Выбор достижения для снятия"""
    markup = types.InlineKeyboardMarkup(row_width=2)

    buttons = []
    for achievement_id, emoji in ACHIEVEMENT_EMOJIS.items():
        buttons.append(
            types.InlineKeyboardButton(
                f"{emoji} {achievement_id}",
                callback_data=f'remove_achievement_{achievement_id}'
            )
        )

    # Разбиваем на строки по 2 кнопки
    for i in range(0, len(buttons), 2):
        if i + 1 < len(buttons):
            markup.add(buttons[i], buttons[i + 1])
        else:
            markup.add(buttons[i])

    markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='admin_achievements'))

    bot.send_message(
        chat_id,
        "<b>🗑️ Выберите достижение для снятия:</b>",
        parse_mode='HTML',
        reply_markup=markup
    )
def process_remove_achievement_reason(message, user_id, achievement_id, original_chat_id):
    """Обработка причины снятия достижения"""
    reason = message.text.strip()
    if reason == '-':
        reason = ""

    try:
        success, result_message = remove_achievement(user_id, achievement_id,
                                                     message.from_user.id, reason)

        if success:
            user = get_user_info(user_id)
            if user:
                city_emoji = AVAILABLE_CITIES.get(user['city'], '🏙️')
                bot.send_message(
                    original_chat_id,
                    f"✅ <b>Достижение снято!</b>\n\n"
                    f"<b>Участник:</b> {user['first_name']} ({city_emoji} {user['city']})\n"
                    f"<b>Достижение:</b> {ACHIEVEMENT_EMOJIS.get(achievement_id, '🏆')} {achievement_id}\n"
                    f"<b>Причина:</b> {reason if reason else 'не указана'}",
                    parse_mode='HTML'
                )
            else:
                bot.send_message(
                    original_chat_id,
                    f"✅ <b>Достижение снято!</b>\n\n"
                    f"<b>Пользователь ID:</b> {user_id}\n"
                    f"<b>Достижение:</b> {ACHIEVEMENT_EMOJIS.get(achievement_id, '🏆')} {achievement_id}\n"
                    f"<b>Причина:</b> {reason if reason else 'не указана'}",
                    parse_mode='HTML'
                )
        else:
            bot.send_message(original_chat_id, f"❌ {result_message}")

        # Возвращаем в панель достижений
        show_achievements_admin_panel(original_chat_id)

    except Exception as e:
        bot.send_message(original_chat_id, f"❌ Ошибка при снятии достижения: {str(e)}")
        show_achievements_admin_panel(original_chat_id)

# Функции для планёрок
def add_meeting_participation(user_id, meeting_topic, admin_id, notes=""):
    """Добавить запись о посещении планёрки"""
    conn = get_db_connection()
    cursor = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meeting_date = datetime.now().strftime("%Y-%m-%d")

    # Добавляем в историю планёрок
    cursor.execute('''
        INSERT INTO meetings_history (user_id, meeting_date, meeting_topic, added_by_admin, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (user_id, meeting_date, meeting_topic, admin_id, notes, now))

    # Обновляем счётчик планёрок
    new_value = update_user_counter(user_id, 'meetings_attended', 1)

    conn.commit()

    return new_value
def get_meetings_statistics():
    """Получить статистику по планёркам"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Общая статистика
    cursor.execute('SELECT COUNT(*) as total_meetings FROM meetings_history')
    total_meetings = cursor.fetchone()['total_meetings']

    cursor.execute('SELECT COUNT(DISTINCT user_id) as unique_participants FROM meetings_history')
    unique_participants = cursor.fetchone()['unique_participants']

    # Самые активные участники планёрок
    cursor.execute('''
        SELECT u.user_id, u.first_name, u.city, COUNT(mh.id) as meetings_count
        FROM users u
        JOIN meetings_history mh ON u.user_id = mh.user_id
        GROUP BY u.user_id
        ORDER BY meetings_count DESC
        LIMIT 10
    ''')
    top_participants = cursor.fetchall()

    return {
        'total_meetings': total_meetings,
        'unique_participants': unique_participants,
        'top_participants': top_participants
    }
def show_meeting_addition_panel(chat_id, user_id=None):
    """Панель для добавления планёрки"""
    if user_id:
        # Показываем форму добавления планёрки для конкретного пользователя
        msg = bot.send_message(
            chat_id,
            f"📝 <b>Добавление планёрки</b>\n\n"
            f"Введите дату планёрки:",
            parse_mode='HTML'
        )
        bot.register_next_step_handler(msg, process_meeting_topic, user_id, chat_id)
    else:
        # Сначала выбираем пользователя
        show_users_for_achievement(chat_id, 'add_meeting_detail')
def process_meeting_topic(message, user_id, original_chat_id):
    """Обработка темы планёрки"""
    meeting_topic = message.text.strip()

    if not meeting_topic:
        bot.send_message(original_chat_id, "❌ Дата планёрки не может быть пустой")
        return

    # Запрашиваем заметки (опционально)
    msg = bot.send_message(
        original_chat_id,
        f"📝 <b>Дата планёрки:</b> {meeting_topic}\n\n"
        f"Введите заметки (или отправьте '-' чтобы пропустить):",
        parse_mode='HTML'
    )
    bot.register_next_step_handler(msg, process_meeting_notes, user_id, meeting_topic, original_chat_id)
def process_meeting_notes(message, user_id, meeting_topic, original_chat_id):
    """Обработка заметок к планёрке"""
    notes = message.text.strip()
    if notes == '-':
        notes = ""

    # Добавляем планёрку
    new_count = add_meeting_participation(user_id, meeting_topic, message.from_user.id, notes)

    user = get_user_info(user_id)
    city_emoji = AVAILABLE_CITIES.get(user['city'], '🏙️')

    bot.send_message(
        original_chat_id,
        f"✅ <b>Планёрка добавлена!</b>\n\n"
        f"<b>Участник:</b> {user['first_name']} ({city_emoji} {user['city']})\n"
        f"<b>Дата:</b> {meeting_topic}\n"
        f"<b>Всего планёрок:</b> {new_count}\n"
        f"{f'<b>Заметки:</b> {notes}' if notes else ''}",
        parse_mode='HTML'
    )

    # Возвращаем в панель достижений
    show_achievements_admin_panel(original_chat_id)

# ======================================
# УПРАВЛЕНИЕ ЗАДАЧАМИ ДЛЯ МУНИЦИПАЛИТЕТОВ
# ======================================
def show_city_admin_tasks(chat_id, message_id=None):
    """Показать ВСЕ задачи из Excel с новыми кнопками управления"""
    if not is_admin(chat_id):
        bot.send_message(chat_id, "⛔ Нет доступа")
        return

    # Загружаем задачи из Excel
    tasks, error = load_tasks_from_excel()

    if error:
        response = f"❌ {error}"
    elif not tasks:
        response = "📋 <b>Задачи муниципалитетам</b>\n\nНет задач в Excel файле."
    else:
        # Счетчики для статистики
        total_tasks = len(tasks)
        assigned_tasks = 0
        unassigned_tasks = 0

        for task in tasks:
            responsible = str(task.get('Ответственный', '')).strip()
            if responsible and responsible.lower() not in ['', 'nan', 'none', 'nat']:
                assigned_tasks += 1
            else:
                unassigned_tasks += 1

        response = (
            f"📋 <b>ВСЕ задачи из Excel</b>\n\n"
            f"<b>Всего задач:</b> {total_tasks}\n"
            f"<b>Назначенные:</b> {assigned_tasks}\n"
            f"<b>Без ответственного:</b> {unassigned_tasks}\n\n"
        )

        # Показываем все задачи
        response += "<b>📌 Все задачи (первые 15):</b>\n"
        for i, task in enumerate(tasks[:15], 1):
            city = task.get('Ответственный', '')
            if not city or str(city).lower() in ['', 'nan', 'none', 'nat']:
                city_display = "Не назначен"
                city_emoji = "❌"
            else:
                city_display = city
                city_emoji = AVAILABLE_CITIES.get(city, '🏙️')

            date_str = task.get('Дата', 'Без срока')
            response += (
                f"{i}. <b>{task['Задача'][:30]}</b>\n"
                f"   {city_emoji} {city_display} | 📅 {date_str}\n"
            )

        if total_tasks > 15:
            response += f"\n<i>... и еще {total_tasks - 15} задач</i>"

    # ОБНОВЛЕННАЯ КЛАВИАТУРА с новыми кнопками
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton('➕ Поставить задачу', callback_data='admin_add_task_city'),
        types.InlineKeyboardButton('✅ Отметить выполненной', callback_data='admin_complete_task_menu')
    )
    markup.add(
        types.InlineKeyboardButton('🗑️ Снять ответственного', callback_data='admin_clear_responsible_menu'),
        types.InlineKeyboardButton('📊 Статистика', callback_data='admin_tasks_stats')
    )
    markup.add(
        types.InlineKeyboardButton('🚀 Распуш', callback_data='admin_create_raspush'),
        types.InlineKeyboardButton('📊 Отчет о распуше', callback_data='admin_raspush_report'),
        types.InlineKeyboardButton('🗑️ Удалить распуш', callback_data='admin_delete_raspush_menu')
    )
    markup.add(
        types.InlineKeyboardButton('🔙 В админ-панель', callback_data='admin_panel')
    )

    if message_id:
        bot.edit_message_text(response, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
    else:
        bot.send_message(chat_id, response, parse_mode='HTML', reply_markup=markup)
def add_city_task(task_name, description, city, admin_id, due_date=None, points=0):
    """Добавить задачу для муниципалитета"""
    conn = get_db_connection()
    cursor = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    due_date_str = due_date.strftime("%Y-%m-%d %H:%M:%S") if due_date else None

    if city == "ALL":
        # Добавляем задачу для всех муниципалитетов
        task_ids = []
        for city_name in AVAILABLE_CITIES.keys():
            cursor.execute('''
                INSERT INTO bot_tasks 
                (task_name, task_description, assigned_city, assigned_by_admin, 
                 assigned_date, due_date, points_reward, is_all_cities, deadline_notified)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (task_name, description, city_name, admin_id, now, due_date_str, points, 1, 0))

            task_ids.append(cursor.lastrowid)

            # Записываем в Excel для каждого муниципалитета
            success, excel_message = add_task_to_excel(task_name, description, city_name, due_date)
            if not success:
                print(f"Внимание для {city_name}: Не удалось записать в Excel: {excel_message}")

            # Уведомляем пользователей муниципалитета
            notify_city_about_task(city_name, task_name, description, due_date_str, points)

        conn.commit()
        return task_ids
    else:
        # Добавляем задачу для одного муниципалитета
        cursor.execute('''
            INSERT INTO bot_tasks 
            (task_name, task_description, assigned_city, assigned_by_admin, 
             assigned_date, due_date, points_reward, is_all_cities, deadline_notified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (task_name, description, city, admin_id, now, due_date_str, points, 0, 0))

        task_id = cursor.lastrowid

        # Записываем в Excel
        success, excel_message = add_task_to_excel(task_name, description, city, due_date)
        if not success:
            print(f"Внимание: Не удалось записать в Excel: {excel_message}")

        conn.commit()

        # Уведомляем пользователей
        notify_city_about_task(city, task_name, description, due_date_str, points)

        return task_id
def notify_city_about_task(city, task_name, description, due_date, points, task_id=None):
    """Уведомить всех пользователей муниципалитета о новой задаче"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT user_id FROM users WHERE city = ? AND is_banned = 0', (city,))
    users = cursor.fetchall()

    city_emoji = AVAILABLE_CITIES.get(city, '🏙️')

    for user in users:
        try:
            message = (
                f"{city_emoji} <b>НОВАЯ ЗАДАЧА ДЛЯ {city}</b>\n\n"
                f"<b>{task_name}</b>\n\n"
            )

            if description:
                message += f"<b>Описание:</b>\n{description}\n\n"

            # ИСПРАВЬ ЭТУ ЧАСТЬ - используй параметр due_date, а не due_date_str
            if due_date:  # ← проверяем due_date, не due_date_str
                due_date_obj = datetime.strptime(due_date, "%Y-%m-%d %H:%M:%S")
                formatted_date = due_date_obj.strftime("%d.%m.%Y в %H:%M")
                message += f"<b>Срок выполнения:</b> до {formatted_date}\n"

            if points > 0:
                message += f"<b>Награда:</b> 🏅 +{points} баллов\n\n"

            message += "<i>Задача отмечена в вашем списке «Мои задачи»</i>"

            bot.send_message(user['user_id'], message, parse_mode='HTML')

        except Exception as e:
            print(f"Не удалось отправить уведомление пользователю {user['user_id']}: {e}")
def complete_city_task(task_id, admin_id, reason="", action="complete", points=0):
    """Отметить задачу как выполненную или снять её с опцией добавления/списания баллов"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM bot_tasks WHERE id = ?', (task_id,))
    task = cursor.fetchone()

    if not task:
        return False, "Задача не найдена"

    if task['is_completed'] == 1:
        return False, "Задача уже выполнена"

    # 1. УДАЛЯЕМ ИЗ EXCEL ПЕРЕД ОБНОВЛЕНИЕМ БД
    try:
        # Загружаем Excel файл
        df = pd.read_excel(EXCEL_FILE_PATH, engine='openpyxl')

        task_name = task['task_name']
        assigned_city = task['assigned_city']

        # Ищем задачу в Excel
        mask = (
                df['Задача'].astype(str).str.contains(task_name, case=False, na=False) &
                (df['Ответственный'].astype(str) == assigned_city)
        )

        if mask.any():
            # Удаляем найденную задачу
            df = df[~mask]
            df.to_excel(EXCEL_FILE_PATH, index=False)
            excel_result = " (удалена из Excel)"
        else:
            excel_result = " (не найдена в Excel)"

    except Exception as e:
        print(f"Ошибка при удалении из Excel: {e}")
        excel_result = " (ошибка при удалении из Excel)"

    # 2. ОБНОВЛЯЕМ БД
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute('''
        UPDATE bot_tasks 
        SET is_completed = 1, completed_date = ?
        WHERE id = ?
    ''', (now, task_id))

    # Обработка баллов в зависимости от действия
    points_to_award = 0

    if action == "complete" and task['points_reward'] > 0:
        points_to_award = task['points_reward']
    elif action == "add_points":
        points_to_award = points
    elif action == "remove_points":
        points_to_award = -abs(points)  # Отрицательное значение

    if points_to_award != 0:
        cursor.execute('''
            UPDATE users 
            SET points = points + ? 
            WHERE city = ? AND is_banned = 0
        ''', (points_to_award, task['assigned_city']))

        cursor.execute('SELECT user_id FROM users WHERE city = ?', (task['assigned_city'],))
        users = cursor.fetchall()

        for user in users:
            log_points_history(
                user['user_id'],
                points_to_award,
                f"Снятие задачи: {task['task_name']} ({reason})",
                admin_id
            )

    conn.commit()

    # 3. ФОРМИРУЕМ ОТВЕТ
    points_message = ""
    if action == "complete" and task['points_reward'] > 0:
        points_message = f"Начислено баллов: {task['points_reward']}"
    elif action == "add_points":
        points_message = f"Дополнительно начислено: {points} баллов"
    elif action == "remove_points":
        points_message = f"Списано баллов: {points}"

    return True, f"✅ Задача снята. {points_message}{excel_result}"

    return True, f"Задача снята. {f'Начислено баллов: {points_to_award}' if points_to_award > 0 else f'Списано баллов: {abs(points_to_award)}' if points_to_award < 0 else ''}"
def notify_city_about_task_completion(city, task_name, points):
    """Уведомить муниципалитет о выполнении задачи"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT user_id FROM users WHERE city = ? AND is_banned = 0', (city,))
    users = cursor.fetchall()

    city_emoji = AVAILABLE_CITIES.get(city, '🏙️')

    for user in users:
        try:
            message = (
                f"{city_emoji} <b>ЗАДАЧА ВЫПОЛНЕНА!</b>\n\n"
                f"<b>{task_name}</b>\n\n"
                f"<i>Задача для {city} отмечена как выполненная</i>\n"
            )

            if points > 0:
                message += f"\n<b>🎁 Награда:</b> 🏅 +{points} баллов каждому участнику!"

            bot.send_message(user['user_id'], message, parse_mode='HTML')

        except Exception as e:
            print(f"Не удалось отправить уведомление пользователю {user['user_id']}: {e}")
def send_completion_result(chat_id, success, result_message, task_id):
    """Отправка результата снятия задачи"""
    if success:
        bot.send_message(
            chat_id,
            f"✅ <b>Задача #{task_id} снята!</b>\n\n"
            f"{result_message}\n\n"
            f"<i>Участники муниципалитета получили уведомление</i>",
            parse_mode='HTML'
        )
    else:
        bot.send_message(chat_id, f"❌ {result_message}")

    show_city_admin_tasks(chat_id)
def start_add_city_task_dialog(chat_id):
    """Начать диалог добавления задачи"""
    msg = bot.send_message(
        chat_id,
        "➕ <b>Добавление задачи муниципалитету</b>\n\n"
        "Введите название задачи:",
        parse_mode='HTML'
    )
    bot.register_next_step_handler(msg, process_task_name_step)
def process_task_name_step(message):
    """Обработка названия задачи"""
    task_name = message.text.strip()
    if not task_name:
        bot.send_message(message.chat.id, "❌ Название не может быть пустым")
        start_add_city_task_dialog(message.chat.id)
        return

    # Сохраняем во временном хранилище
    broadcast_cache[f"task_name_{message.from_user.id}"] = task_name

    # Запрашиваем описание
    msg = bot.send_message(
        message.chat.id,
        f"📝 <b>Название:</b> {task_name}\n\n"
        "Введите описание задачи (или '-' для пропуска):",
        parse_mode='HTML'
    )
    bot.register_next_step_handler(msg, process_task_description_step)
def process_task_description_step(message):
    """Обработка описания задачи"""
    description = message.text.strip()
    if description == '-':
        description = ""

    # Сохраняем описание
    broadcast_cache[f"task_desc_{message.from_user.id}"] = description

    # Запрашиваем срок выполнения
    msg = bot.send_message(
        message.chat.id,
        "📅 <b>Срок выполнения</b>\n\n"
        "Введите дату и время в формате:\n"
        "<code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n\n"
        "<i>Пример: 15.03.2024 18:00</i>\n\n"
        "Или введите '-' для задачи без срока:",
        parse_mode='HTML'
    )
    bot.register_next_step_handler(msg, process_task_due_date)
def process_task_due_date(message):
    """Обработка срока выполнения"""
    due_text = message.text.strip()

    if due_text == '-':
        due_date = None
        due_date_str = None
    else:
        try:
            due_date = datetime.strptime(due_text, "%d.%m.%Y %H:%M")
            due_date_str = due_date.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            bot.send_message(
                message.chat.id,
                "❌ Неверный формат даты. Используйте: ДД.ММ.ГГГГ ЧЧ:ММ\n"
                "Пример: 15.03.2024 18:00"
            )
            process_task_description_step(message)
            return

    # Сохраняем дату
    broadcast_cache[f"task_due_{message.from_user.id}"] = due_date_str
    broadcast_cache[f"task_due_obj_{message.from_user.id}"] = due_date

    # Показываем выбор муниципалитета
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(types.InlineKeyboardButton('🌍 Все муниципалитеты', callback_data='select_task_city_ALL_MUNICIPALITIES'))

    for city, emoji in AVAILABLE_CITIES.items():
        markup.add(types.InlineKeyboardButton(f"{emoji} {city}", callback_data=f'select_task_city_{city}'))

    markup.add(types.InlineKeyboardButton('❌ Отмена', callback_data='admin_city_tasks'))

    bot.send_message(
        message.chat.id,
        "🏙️ <b>Выберите муниципалитет:</b>\n\n"
        "<i>Выберите конкретный муниципалитет или 'Все муниципалитеты' для массовой задачи</i>",
        parse_mode='HTML',
        reply_markup=markup
    )
def process_task_city_selection(call, city):
    """Обработка выбора муниципалитета"""
    # Получаем сохраненные данные
    task_name = broadcast_cache.get(f"task_name_{call.from_user.id}")
    description = broadcast_cache.get(f"task_desc_{call.from_user.id}")
    due_date_str = broadcast_cache.get(f"task_due_{call.from_user.id}")
    due_date = broadcast_cache.get(f"task_due_obj_{call.from_user.id}")

    if not task_name:
        bot.answer_callback_query(call.id, "❌ Ошибка: данные не найдены")
        return

    # Сохраняем муниципалитет
    if city == "ALL_MUNICIPALITIES":
        broadcast_cache[f"task_city_{call.from_user.id}"] = "ALL"
        broadcast_cache[f"task_all_cities_{call.from_user.id}"] = True
    else:
        broadcast_cache[f"task_city_{call.from_user.id}"] = city
        broadcast_cache[f"task_all_cities_{call.from_user.id}"] = False

    # Пропускаем выбор даты и сразу переходим к награде
    process_reward_selection(call)
def process_reward_selection(call):
    """Переход к выбору награды после выбора даты"""
    # Получаем все данные
    task_name = broadcast_cache.get(f"task_name_{call.from_user.id}")
    city = broadcast_cache.get(f"task_city_{call.from_user.id}")
    due_date_str = broadcast_cache.get(f"task_due_{call.from_user.id}")
    is_all_cities = broadcast_cache.get(f"task_all_cities_{call.from_user.id}", False)

    if due_date_str:
        # ИСПРАВЬ ЭТУ СТРОКУ (строка 2240):
        due_date = datetime.strptime(due_date_str, "%Y-%m-%d %H:%M:%S")  # ← было "%Y-%m-%d"
        due_date_display = due_date.strftime("%d.%m.%Y в %H:%M")
    else:
        due_date_display = "без срока"

    # Создаем клавиатуру для выбора награды
    markup = types.InlineKeyboardMarkup(row_width=3)
    markup.add(
        types.InlineKeyboardButton('0 баллов', callback_data='task_points_0'),
        types.InlineKeyboardButton('5 баллов', callback_data='task_points_5'),
        types.InlineKeyboardButton('10 баллов', callback_data='task_points_10'),
        types.InlineKeyboardButton('15 баллов', callback_data='task_points_15'),
        types.InlineKeyboardButton('20 баллов', callback_data='task_points_20'),
        types.InlineKeyboardButton('25 баллов', callback_data='task_points_25'),
        types.InlineKeyboardButton('30 баллов', callback_data='task_points_30'),
        types.InlineKeyboardButton('❌ Отмена', callback_data='admin_city_tasks')
    )

    city_emoji = "🌍" if is_all_cities else AVAILABLE_CITIES.get(city, '🏙️')
    city_name = "все муниципалитеты" if is_all_cities else city

    bot.edit_message_text(
        f"🏅 <b>Награда за выполнение</b>\n\n"
        f"<b>Задача:</b> {task_name}\n"
        f"<b>Муниципалитет:</b> {city_emoji} {city_name}\n"
        f"<b>Срок:</b> {due_date_display}\n\n"
        "Выберите награду в баллах:",
        call.message.chat.id,
        call.message.message_id,
        parse_mode='HTML',
        reply_markup=markup
    )
def process_task_points_selection(call, points):
    """Обработка награды и сохранение задачи"""
    # Получаем все данные
    task_name = broadcast_cache.get(f"task_name_{call.from_user.id}")
    description = broadcast_cache.get(f"task_desc_{call.from_user.id}")
    city = broadcast_cache.get(f"task_city_{call.from_user.id}")
    due_date_str = broadcast_cache.get(f"task_due_{call.from_user.id}")

    due_date = datetime.strptime(due_date_str, "%Y-%m-%d %H:%M:%S")if due_date_str else None

    # Добавляем задачу
    task_id = add_city_task(
        task_name=task_name,
        description=description,
        city=city,
        admin_id=call.from_user.id,
        due_date=due_date,
        points=int(points)
    )

    # Очищаем кэш
    for key in [f"task_name_{call.from_user.id}", f"task_desc_{call.from_user.id}",
                f"task_city_{call.from_user.id}", f"task_due_{call.from_user.id}"]:
        if key in broadcast_cache:
            del broadcast_cache[key]

    # Показываем подтверждение
    city_emoji = AVAILABLE_CITIES.get(city, '🏙️')
    due_date_display = due_date.strftime("%d.%m.%Y") if due_date else "без срока"

    bot.edit_message_text(
        f"✅ <b>ЗАДАЧА ДОБАВЛЕНА!</b>\n\n"
        f"<b>Название:</b> {task_name}\n"
        f"<b>Муниципалитет:</b> {city_emoji} {city}\n"
        f"<b>Срок:</b> {due_date_display}\n"
        f"<b>Награда:</b> 🏅 +{points} баллов\n"
        f"<b>ID задачи:</b> <code>{task_id}</code>\n\n"
        f"<i>Все участники {city} получили уведомление</i>",
        call.message.chat.id,
        call.message.message_id,
        parse_mode='HTML',
        reply_markup=types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton('📋 К списку задач', callback_data='admin_city_tasks')
        )
    )


def check_task_deadlines():
    """Проверка дедлайнов задач и отправка уведомлений"""
    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Используем текущее время и время через 24 часа
            now = datetime.now()
            today_str = now.strftime("%Y-%m-%d %H:%M:%S")
            tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

            cursor.execute('''
                SELECT id, task_name, assigned_city, due_date
                FROM bot_tasks 
                WHERE is_completed = 0 
                AND due_date IS NOT NULL
                AND due_date > ?
                AND due_date <= ?
                AND deadline_notified = 0
            ''', (today_str, tomorrow))

            tasks = cursor.fetchall()

            for task in tasks:
                notify_task_deadline_reminder(task)

                cursor.execute('''
                    UPDATE bot_tasks 
                    SET deadline_notified = 1 
                    WHERE id = ?
                ''', (task['id'],))

            conn.commit()

        except Exception as e:
            print(f"Ошибка при проверке дедлайнов: {e}")
            import traceback
            print(traceback.format_exc())

        # Проверяем каждые 30 минут
        time.sleep(1800)

def notify_task_deadline_reminder(task):
    """Уведомление о приближении дедлайна"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('SELECT user_id FROM users WHERE city = ? AND is_banned = 0', (task['assigned_city'],))
    users = cursor.fetchall()

    city_emoji = AVAILABLE_CITIES.get(task['assigned_city'], '🏙️')
    due_date = datetime.strptime(task['due_date'], "%Y-%m-%d %H:%M:%S")
    formatted_date = due_date.strftime("%d.%m.%Y в %H:%M")

    for user in users:
        try:
            message = (
                f"⏰ <b>НАПОМИНАНИЕ О ДЕДЛАЙНЕ!</b>\n\n"
                f"<b>{city_emoji} {task['assigned_city']}</b>\n"
                f"<b>Задача:</b> {task['task_name']}\n"
                f"<b>Срок выполнения:</b> {formatted_date}\n\n"
                f"<i>Осталось менее 24 часов!</i>"
            )

            bot.send_message(user['user_id'], message, parse_mode='HTML')
        except Exception as e:
            print(f"Не удалось отправить напоминание пользователю {user['user_id']}: {e}")
def assign_task_to_user(user_id, task_index_in_all):
    """Назначить задачу пользователю (добавить его муниципалитет в Ответственный)"""
    try:
        user = get_user_info(user_id)
        if not user:
            return False, "Пользователь не найден"

        user_city = user['city']

        # Загружаем все задачи с правильными типами данных
        tasks, error = load_tasks_from_excel()
        if error:
            return False, error

        if task_index_in_all >= len(tasks):
            return False, "Задача не найдена"

        task = tasks[task_index_in_all]

        # ОБНОВЛЕННАЯ ЧАСТЬ: Читаем Excel с правильными типами данных
        file_path = EXCEL_FILE_PATH
        if not os.path.exists(file_path):
            return False, "Файл не найден"

        # Читаем файл с указанием типа для столбца "Ответственный"
        df = pd.read_excel(
            file_path,
            engine='openpyxl',
            dtype={'Ответственный': str}  # ← ВАЖНО: читаем как строку
        )

        # Преобразуем все значения "Ответственный" в строки и очищаем
        df['Ответственный'] = df['Ответственный'].astype(str).str.strip()

        # Заменяем NaN и специальные значения
        df['Ответственный'] = df['Ответственный'].replace({
            'nan': '',
            'None': '',
            'NaN': '',
            '<NA>': '',
            'NaT': '',
            'None': ''
        })

        # Находим задачу
        task_name = task['Задача']
        task_date = task['Дата']

        # Ищем строку (учитываем, что дата может быть в разных форматах)
        mask = (df['Задача'].astype(str).str.strip() == task_name.strip())

        if task_date:
            # Сравниваем только даты, игнорируя время и формат
            try:
                # Пробуем разные форматы дат
                if isinstance(task_date, str):
                    task_date_str = task_date
                else:
                    task_date_str = str(task_date)

                # Ищем частичное совпадение для даты
                mask = mask & (df['Дата'].astype(str).str.contains(task_date_str.split()[0]))
            except:
                pass

        if mask.any():
            # Нашли задачу - обновляем
            df.loc[mask, 'Ответственный'] = user_city
            df.to_excel(file_path, index=False)

            # Логируем действие
            conn = get_db_connection()
            cursor = conn.cursor()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute('''
                INSERT INTO points_history (user_id, amount, reason, admin_id, date)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, 0, f"Принял задачу: {task_name}", user_id, now))
            conn.commit()

            return True, f"✅ Задача '{task_name}' назначена на ваш муниципалитет ({user_city})"
        else:
            return False, "Не удалось найти задачу в файле"

    except Exception as e:
        return False, f"Ошибка при назначении задачи: {str(e)[:100]}"
def show_complete_task_menu(chat_id):
    """Показать меню для отметки задачи выполненной"""
    # Загружаем задачи с ответственными
    tasks, error = load_tasks_from_excel()

    if error:
        bot.send_message(chat_id, f"❌ {error}")
        return

    # Фильтруем задачи с ответственным
    assigned_tasks = []
    for idx, task in enumerate(tasks):
        responsible = str(task.get('Ответственный', '')).strip()
        if responsible and responsible.lower() not in ['', 'nan', 'none', 'nat']:
            assigned_tasks.append((idx, task))

    if not assigned_tasks:
        bot.send_message(
            chat_id,
            "📭 <b>Нет назначенных задач</b>\n\n"
            "Все задачи в Excel не имеют ответственного.",
            parse_mode='HTML',
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton('🔙 Назад', callback_data='admin_city_tasks')
            )
        )
        return

    # Создаем клавиатуру с задачами
    markup = types.InlineKeyboardMarkup(row_width=2)

    for idx, task in assigned_tasks[:20]:  # Показываем первые 20
        city = task.get('Ответственный', 'Не указан')
        task_name_short = task['Задача'][:20] + ("..." if len(task['Задача']) > 20 else "")
        city_emoji = AVAILABLE_CITIES.get(city, '🏙️')

        markup.add(types.InlineKeyboardButton(
            f"{city_emoji} {task_name_short}",
            callback_data=f'complete_task_{idx}'
        ))

    markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='admin_city_tasks'))

    bot.send_message(
        chat_id,
        "✅ <b>Выберите задачу для отметки выполненной:</b>\n\n"
        "<i>Задача будет удалена из Excel, счётчик пользователя обновится</i>",
        parse_mode='HTML',
        reply_markup=markup
    )
def show_clear_responsible_menu(chat_id):
    """Показать меню для снятия ответственного"""
    # Загружаем задачи с ответственными
    tasks, error = load_tasks_from_excel()

    if error:
        bot.send_message(chat_id, f"❌ {error}")
        return

    # Фильтруем задачи с ответственным
    assigned_tasks = []
    for idx, task in enumerate(tasks):
        responsible = str(task.get('Ответственный', '')).strip()
        if responsible and responsible.lower() not in ['', 'nan', 'none', 'nat']:
            assigned_tasks.append((idx, task))

    if not assigned_tasks:
        bot.send_message(
            chat_id,
            "📭 <b>Нет назначенных задач</b>\n\n"
            "Все задачи в Excel не имеют ответственного.",
            parse_mode='HTML',
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton('🔙 Назад', callback_data='admin_city_tasks')
            )
        )
        return

    # Создаем клавиатуру с задачами
    markup = types.InlineKeyboardMarkup(row_width=2)

    for idx, task in assigned_tasks[:20]:  # Показываем первые 20
        city = task.get('Ответственный', 'Не указан')
        task_name_short = task['Задача'][:20] + ("..." if len(task['Задача']) > 20 else "")
        city_emoji = AVAILABLE_CITIES.get(city, '🏙️')

        markup.add(types.InlineKeyboardButton(
            f"{city_emoji} {task_name_short}",
            callback_data=f'clear_responsible_{idx}'
        ))

    markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='admin_city_tasks'))

    bot.send_message(
        chat_id,
        "🗑️ <b>Выберите задачу для снятия ответственного:</b>\n\n"
        "<i>Поле 'Ответственный' будет очищено</i>",
        parse_mode='HTML',
        reply_markup=markup
    )

# ==============================
# 6. ОБРАБОТЧИКИ КОМАНД
# ==============================
@bot.message_handler(commands=['start'])
def main(message):
    """Команда /start"""
    user_id = message.from_user.id

    # Проверяем регистрацию
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    user = cursor.fetchone()

    if not user:
        show_city_selection(user_id, message.chat.id)
        return

    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [types.InlineKeyboardButton('👤 Личный кабинет', callback_data='personal_cabinet')]

    if is_admin(user_id):
        buttons.append(types.InlineKeyboardButton('⚙️ Админ-панель', callback_data='admin_panel'))

    markup.add(*buttons)
    bot.send_message(message.chat.id,
                     f'<b>Привет, {message.from_user.first_name}!</b> \U0001F44B \n\nЭтот бот – твой помощник. Здесь ты можешь увидеть актуальные задачи, количество баллов и актуальный рейтинг шеф-редакций в области. \n\nХорошего дня! \U0001F496',
                     parse_mode='HTML', reply_markup=markup)

@bot.message_handler(commands=['cabinet'])
def cabinet_command(message):
    """Команда /cabinet"""
    show_personal_cabinet(message.from_user.id, message.chat.id)

@bot.message_handler(commands=['setcity'])
def set_city_command(message):
    """Команда /setcity"""
    user_id = message.from_user.id

    markup = types.InlineKeyboardMarkup(row_width=2)
    for city, emoji in AVAILABLE_CITIES.items():
        markup.add(types.InlineKeyboardButton(f"{emoji} {city}", callback_data=f'change_city_{city}'))

    bot.send_message(message.chat.id, "🏙️ <b>Выберите муниципалитет:</b>",
                     parse_mode='HTML', reply_markup=markup)

@bot.message_handler(commands=['admin'])
def admin_command(message):
    """Команда /admin"""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "⛔ Нет доступа")
        return
    show_admin_panel(message.chat.id)

@bot.message_handler(commands=['setrules'])
def set_rules_command(message):
    """Установить правила работы"""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "⛔ Нет доступа")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        bot.reply_to(message, "Используйте: /setrules [текст правил]")
        return

    rules_text = args[1]
    save_rules(rules_text)
    bot.reply_to(message, "✅ Правила успешно обновлены")

@bot.message_handler(commands=['setcontentplan'])
def set_content_plan_command(message):
    """Установить контент-план"""
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "⛔ Нет доступа")
        return

    if not message.photo and not message.document:
        bot.reply_to(message,
                     "📤 <b>Отправьте изображение для контент-плана</b>\n\n"
                     "Можно отправить как фото (с подписью) или как документ.",
                     parse_mode='HTML')
        return

    # Сохраняем информацию о контент-плане
    file_id, caption = save_content_plan_info(message)

    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton('👀 Посмотреть', callback_data='show_content_plan'),
        types.InlineKeyboardButton('⚙️ Админ-панель', callback_data='admin_panel')
    )

    bot.reply_to(
        message,
        f"✅ <b>Контент-план успешно обновлен!</b>\n\n"
        f"<b>Подпись:</b> {caption}\n"
        f"<b>File ID:</b> <code>{file_id}</code>\n\n"
        f"Теперь пользователи смогут увидеть его в личном кабинете.",
        parse_mode='HTML',
        reply_markup=markup
    )

@bot.message_handler(commands=['stats'])
def stats_command(message):
    """Показать статистику"""
    if not is_admin(message.from_user.id):
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    # Общая статистика
    cursor.execute('SELECT COUNT(DISTINCT user_id) as total_users FROM users')
    total_users = cursor.fetchone()['total_users']

    cursor.execute('SELECT COUNT(*) as total_achievements FROM user_achievements')
    total_achievements = cursor.fetchone()['total_achievements']

    cursor.execute('''
        SELECT counter_type, SUM(value) as total 
        FROM user_counters 
        GROUP BY counter_type
    ''')
    counters_stats = cursor.fetchall()

    response = f"<b>📊 Статистика достижений</b>\n\n"
    response += f"<b>Всего пользователей:</b> {total_users}\n"
    response += f"<b>Всего достижений выдано:</b> {total_achievements}\n\n"

    response += "<b>Счётчики:</b>\n"
    for stat in counters_stats:
        counter_name = COUNTERS_CONFIG.get(stat['counter_type'], {}).get('name', stat['counter_type'])
        response += f"• {counter_name}: {stat['total']}\n"

    bot.reply_to(message, response, parse_mode='HTML')

# ==============================
# 7. ОБРАБОТЧИКИ КНОПОК
# ==============================
# ==============================
# ОБРАБОТЧИКИ ДЛЯ РУЧНОГО ВВОДА ID
# ==============================

@bot.callback_query_handler(func=lambda call: call.data.startswith('manual_id_'))
def handle_manual_id(call):
    """Обработчик ручного ввода ID пользователя"""
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "⛔ Нет доступа")
        return

    action = call.data.replace('manual_id_', '')

    msg = bot.send_message(
        call.message.chat.id,
        f"✏️ <b>Введите ID пользователя для {('начисления' if action == 'add' else 'снятия')} баллов:</b>\n\n"
        f"<i>ID можно узнать в списке пользователей</i>",
        parse_mode='HTML'
    )

    bot.register_next_step_handler(msg, process_manual_id, action, call.message.chat.id)
    bot.delete_message(call.message.chat.id, call.message.message_id)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    is_admin_user = is_admin(user_id)

    # Регистрация
    if call.data.startswith('select_city_'):
        city = call.data.replace('select_city_', '')
        get_or_create_user(user_id, call.from_user.username, call.from_user.first_name,
                           call.from_user.last_name, city)
        bot.edit_message_text(f"✅ Вы выбрали: {AVAILABLE_CITIES.get(city, '🏙️')} {city}",
                              chat_id, call.message.message_id)

    elif call.data == 'change_city':
        markup = types.InlineKeyboardMarkup(row_width=2)
        for city, emoji in AVAILABLE_CITIES.items():
            markup.add(types.InlineKeyboardButton(f"{emoji} {city}", callback_data=f'change_city_{city}'))
        bot.edit_message_text("🏙️ <b>Выберите новый муниципалитет:</b>",
                              chat_id, call.message.message_id,
                              parse_mode='HTML', reply_markup=markup)

    elif call.data.startswith('change_city_'):
        city = call.data.replace('change_city_', '')
        if update_user_city(user_id, city):
            city_emoji = AVAILABLE_CITIES.get(city, '🏙️')
            bot.edit_message_text(f"✅ Муниципалитет изменен на: {city_emoji} {city}",
                                  chat_id, call.message.message_id)

    # Пагинация муниципалитетов
    elif call.data.startswith('city_page_'):
        page = int(call.data.replace('city_page_', ''))
        bot.delete_message(chat_id, call.message.message_id)
        show_city_selection(user_id, chat_id, page)

    # Личный кабинет
    elif call.data == 'personal_cabinet':
        show_personal_cabinet(user_id, chat_id)
    elif call.data == 'user_history':
        # Показываем историю пользователя
        show_user_history(call.from_user.id, call.message.chat.id, call.message.message_id)
    # Админ-панель
    elif call.data == 'admin_panel':
        if is_admin_user:
            show_admin_panel(chat_id)
        else:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
    elif call.data == 'admin_set_rules':
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        rules_text = get_rules()
        bot.edit_message_text(
            "<b>📋 Установка правил работы</b>\n\n"
            "Используйте команду:\n"
            "<code>/setrules [текст правил]</code>\n\n"
            "<b>Пример:</b>\n"
            "<code>/setrules 1. Соблюдать сроки\\n2. Проверять информацию</code>\n\n"
            "Для переноса строк используйте \\n\n\n"
            "<b>Текущие правила:</b>\n"
            f"{rules_text[:200]}..." if len(rules_text) > 200 else rules_text,
            chat_id,
            call.message.message_id,
            parse_mode='HTML',
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton('🔙 Назад', callback_data='admin_panel')
            )
        )
    elif call.data == 'admin_achievements_stats':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем всех пользователей с их достижениями
        cursor.execute('''
                SELECT u.user_id, u.first_name, u.city, 
                       GROUP_CONCAT(ua.achievement_id) as achievements
                FROM users u
                LEFT JOIN user_achievements ua ON u.user_id = ua.user_id
                GROUP BY u.user_id
                ORDER BY u.first_name
            ''')

        users = cursor.fetchall()

        response = "<b>📊 Общая статистика достижений</b>\n\n"

        for user in users:
            if not user['achievements']:
                continue

            # Собираем эмодзи достижений
            achievement_emojis = []
            for ach_id in user['achievements'].split(','):
                if ach_id and ach_id in ACHIEVEMENT_EMOJIS:
                    achievement_emojis.append(ACHIEVEMENT_EMOJIS[ach_id])

            city_emoji = AVAILABLE_CITIES.get(user['city'], '🏙️')
            response += f"{user['first_name']} | {city_emoji} {user['city']} | {' '.join(achievement_emojis)}\n"

        if not response.endswith("\n\n"):
            response += "\n\n"

        response += f"<i>Всего пользователей с достижениями: {len([u for u in users if u['achievements']])}</i>"

        bot.edit_message_text(
            response,
            call.message.chat.id,
            call.message.message_id,
            parse_mode='HTML',
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton('🔙 Назад', callback_data='admin_achievements')
            )
        )
    elif call.data == 'admin_change_city':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        msg = bot.send_message(
            call.message.chat.id,
            "🌐 <b>Изменение муниципалитета</b>\n\n"
            "Введите ID пользователя и новый муниципалитет в формате:\n"
            "<code>ID_пользователя : Муниципалитет</code>\n\n"
            "<i>Пример: 123456 : Москва</i>",
            parse_mode='HTML'
        )

        def process_city_change(message):
            try:
                if ':' not in message.text:
                    bot.send_message(message.chat.id, "❌ Неверный формат. Используйте: ID : Город")
                    return

                user_id_str, new_city = message.text.split(':', 1)
                user_id = int(user_id_str.strip())
                new_city = new_city.strip()

                if new_city not in AVAILABLE_CITIES:
                    bot.send_message(message.chat.id, f"❌ Муниципалитет '{new_city}' не найден")
                    return

                # Меняем муниципалитет
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE users SET city = ? WHERE user_id = ?', (new_city, user_id))
                conn.commit()

                city_emoji = AVAILABLE_CITIES.get(new_city, '🏙️')
                bot.send_message(
                    message.chat.id,
                    f"✅ Муниципалитет пользователя #{user_id} изменен на: {city_emoji} {new_city}"
                )

                # Уведомляем пользователя
                try:
                    bot.send_message(
                        user_id,
                        f"🌐 <b>Ваш муниципалитет изменен!</b>\n\n"
                        f"Администратор изменил ваш муниципалитет на: {city_emoji} {new_city}",
                        parse_mode='HTML'
                    )
                except:
                    pass

                # Возвращаем в админ-панель
                show_admin_panel(message.chat.id)

            except ValueError:
                bot.send_message(message.chat.id, "❌ Ошибка: ID должен быть числом")
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ Ошибка: {str(e)}")

        bot.register_next_step_handler(msg, process_city_change)
    elif call.data == 'admin_view_user_achievements':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        # Запрашиваем ID пользователя
        msg = bot.send_message(
            call.message.chat.id,
            "👤 <b>Введите ID пользователя для просмотра его достижений:</b>",
            parse_mode='HTML'
        )

        def process_user_id(message):
            try:
                target_user_id = int(message.text)
                show_user_achievements(target_user_id, message.chat.id)
            except ValueError:
                bot.send_message(message.chat.id, "❌ Введите числовой ID пользователя")

        bot.register_next_step_handler(msg, process_user_id)
    elif call.data == 'admin_history_report':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        ask_report_period(call.message.chat.id)
    elif call.data == 'admin_set_content_plan':
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        content_info = get_content_plan_info()
        if content_info['file_id']:
            status = "✅ Контент-план загружен"
            preview = f"Подпись: {content_info['caption'][:50]}..."
        else:
            status = "❌ Контент-план не загружен"
            preview = ""

        bot.edit_message_text(
            f"<b>📅 Обновление контент-плана</b>\n\n"
            f"{status}\n{preview}\n\n"
            "Для обновления отправьте изображение с подписью командой:\n"
            "<code>/setcontentplan</code>\n\n"
            "Или просто отправьте новое изображение с подписью в этот чат.\n\n"
            "<i>Бот сохранит изображение и будет показывать его пользователям.</i>",
            chat_id,
            call.message.message_id,
            parse_mode='HTML',
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton('🔙 Назад', callback_data='admin_panel')
            )
        )
    # Начисление/снятие баллов
    elif call.data == 'admin_add_points_menu':
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        bot.delete_message(chat_id, call.message.message_id)
        show_user_selection_for_points(chat_id, 'add')

    elif call.data == 'admin_remove_points_menu':
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        bot.delete_message(chat_id, call.message.message_id)
        show_user_selection_for_points(chat_id, 'remove')

    elif call.data.startswith('select_user_add_') or call.data.startswith('select_user_remove_'):
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        parts = call.data.split('_')
        action, target_user_id = parts[2], int(parts[3])
        bot.delete_message(chat_id, call.message.message_id)
        show_points_amount_selection(chat_id, target_user_id, action)

    elif call.data.startswith('select_points_add_') or call.data.startswith('select_points_remove_'):
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        parts = call.data.split('_')
        action, target_user_id, points = parts[2], int(parts[3]), int(parts[4])
        bot.delete_message(chat_id, call.message.message_id)
        ask_for_reason(chat_id, target_user_id, points, action)

    elif call.data.startswith('custom_points_'):
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        parts = call.data.split('_')
        action, target_user_id = parts[2], int(parts[3])

        msg = bot.send_message(
            chat_id,
            f"✏️ <b>Введите количество баллов:</b>",
            parse_mode='HTML'
        )

        # Вспомогательная функция для обработки ввода
        def process_custom_input(message):
            try:
                points = int(message.text)
                if points <= 0:
                    bot.send_message(chat_id, "❌ Должно быть положительным числом")
                    return
                ask_for_reason(chat_id, target_user_id, points, action)
            except:
                bot.send_message(chat_id, "❌ Введите число")

        bot.register_next_step_handler(msg, process_custom_input)

    # Достижения
    elif call.data == 'admin_achievements':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        bot.delete_message(call.message.chat.id, call.message.message_id)
        show_achievements_admin_panel(call.message.chat.id)

    elif call.data == 'admin_add_task':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        bot.delete_message(call.message.chat.id, call.message.message_id)
        show_users_for_achievement(call.message.chat.id, 'add_task')

    elif call.data == 'admin_add_idea':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        bot.delete_message(call.message.chat.id, call.message.message_id)
        show_users_for_achievement(call.message.chat.id, 'add_idea')

    elif call.data.startswith('achievement_user_add_task_'):
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        # Формат: achievement_user_add_task_123456
        try:
            target_user_id = int(call.data.split('_')[-1])
            update_user_counter(target_user_id, 'completed_tasks', 1)
            bot.answer_callback_query(call.id, "✅ Добавлено ТЗ")

            # Обновляем сообщение или показываем результат
            user = get_user_info(target_user_id)
            if user:
                counters = get_user_counters(target_user_id)
                new_value = counters.get('completed_tasks', 0)

                bot.edit_message_text(
                    f"✅ <b>Добавлено ТЗ</b>\n\n"
                    f"Пользователь: {user['first_name']}\n"
                    f"Выполнено ТЗ: {new_value}\n\n"
                    f"<i>Пользователь получил уведомление о достижении (если разблокировано)</i>",
                    chat_id,
                    call.message.message_id,
                    parse_mode='HTML',
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton('🔙 Назад', callback_data='admin_achievements')
                    )
                )
        except Exception as e:
            bot.answer_callback_query(call.id, f"❌ Ошибка: {str(e)}")

    elif call.data.startswith('achievement_user_add_idea_'):
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        # Формат: achievement_user_add_idea_123456
        try:
            target_user_id = int(call.data.split('_')[-1])
            update_user_counter(target_user_id, 'content_ideas', 1)
            bot.answer_callback_query(call.id, "✅ Добавлена идея")

            user = get_user_info(target_user_id)
            if user:
                counters = get_user_counters(target_user_id)
                new_value = counters.get('content_ideas', 0)

                bot.edit_message_text(
                    f"✅ <b>Добавлена идея</b>\n\n"
                    f"Пользователь: {user['first_name']}\n"
                    f"Идей предложено: {new_value}\n\n"
                    f"<i>Пользователь получил уведомление о достижении (если разблокировано)</i>",
                    chat_id,
                    call.message.message_id,
                    parse_mode='HTML',
                    reply_markup=types.InlineKeyboardMarkup().add(
                        types.InlineKeyboardButton('🔙 Назад', callback_data='admin_achievements')
                    )
                )
        except Exception as e:
            bot.answer_callback_query(call.id, f"❌ Ошибка: {str(e)}")

    elif call.data.startswith('achievement_user_add_meeting_'):
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        # Формат: achievement_user_add_meeting_123456
        # Для планёрок нужен дополнительный ввод данных
        try:
            target_user_id = int(call.data.split('_')[-1])
            user = get_user_info(target_user_id)

            if user:
                # Запрашиваем тему планёрки
                msg = bot.send_message(
                    chat_id,
                    f"📋 <b>Добавление планёрки</b>\n\n"
                    f"Пользователь: {user['first_name']}\n"
                    f"Введите дату планёрки:",
                    parse_mode='HTML'
                )
                bot.register_next_step_handler(msg, process_meeting_topic, target_user_id, chat_id)

                # Удаляем предыдущее сообщение
                bot.delete_message(chat_id, call.message.message_id)
        except Exception as e:
            bot.answer_callback_query(call.id, f"❌ Ошибка: {str(e)}")

    elif call.data == 'show_all_achievements':
        show_user_achievements(call.from_user.id, call.message.chat.id, call.message.message_id)

    elif call.data == 'admin_give_achievement':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        bot.delete_message(call.message.chat.id, call.message.message_id)
        show_custom_achievement_selection(call.message.chat.id)

    elif call.data == 'admin_add_meeting':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        bot.delete_message(call.message.chat.id, call.message.message_id)
        show_meeting_addition_panel(call.message.chat.id)

    elif call.data == 'admin_meetings_stats':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        stats = get_meetings_statistics()

        response = f"<b>📊 Статистика планёрок</b>\n\n"
        response += f"<b>Всего проведено планёрок:</b> {stats['total_meetings']}\n"
        response += f"<b>Уникальных участников:</b> {stats['unique_participants']}\n\n"

        response += "<b>🏆 Топ участников планёрок:</b>\n"
        for i, participant in enumerate(stats['top_participants'], 1):
            city_emoji = AVAILABLE_CITIES.get(participant['city'], '🏙️')
            response += f"{i}. {participant['first_name']} ({city_emoji} {participant['city']}): {participant['meetings_count']} планёрок\n"

        bot.edit_message_text(
            response,
            call.message.chat.id,
            call.message.message_id,
            parse_mode='HTML',
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton('🔙 Назад', callback_data='admin_achievements')
            )
        )

    elif call.data.startswith('give_achievement_'):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        achievement_id = call.data.replace('give_achievement_', '')

        # Сохраняем выбранное достижение в кэше
        cache_key = f"give_achievement_{call.from_user.id}"
        broadcast_cache[cache_key] = achievement_id

        # Теперь выбираем пользователя
        bot.delete_message(call.message.chat.id, call.message.message_id)
        show_users_for_achievement(call.message.chat.id, 'give_manual_achievement')

    elif call.data.startswith('achievement_user_give_manual_achievement'):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        # Формат: achievement_user_give_manual_achievement_123456
        try:
            # Берём всё после последнего подчёркивания
            target_user_id = int(call.data.rsplit('_', 1)[-1])
            # Получаем achievement_id из кэша
            cache_key = f"give_achievement_{call.from_user.id}"
            if cache_key in broadcast_cache:
                achievement_id = broadcast_cache[cache_key]
                # Запрашиваем причину выдачи
                msg = bot.send_message(
                    call.message.chat.id,
                    f"📝 <b>Выдача достижения</b>\n\n"
                    f"<b>Достижение:</b> {get_achievement_emoji(achievement_id)} {achievement_id}\n"
                    f"<b>Получатель ID:</b> {target_user_id}\n\n"
                    f"Введите причину выдачи (или '-' чтобы пропустить):",
                    parse_mode='HTML'
                )
                bot.register_next_step_handler(msg, process_manual_achievement_reason,
                                               target_user_id, achievement_id, call.message.chat.id)
                del broadcast_cache[cache_key]
            else:
                bot.answer_callback_query(call.id, "❌ Ошибка: достижение не выбрано")

        except ValueError as e:
            bot.answer_callback_query(call.id, f"❌ Ошибка формата: {e}")

    elif call.data == 'admin_remove_achievement':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        bot.delete_message(call.message.chat.id, call.message.message_id)
        show_remove_achievement_selection(call.message.chat.id)

    elif call.data.startswith('remove_achievement_'):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        achievement_id = call.data.replace('remove_achievement_', '')

        # Сохраняем в кэше
        cache_key = f"remove_achievement_{call.from_user.id}"
        broadcast_cache[cache_key] = achievement_id

        # Выбираем пользователя
        bot.delete_message(call.message.chat.id, call.message.message_id)
        show_users_for_achievement(call.message.chat.id, 'remove_achievement')

    elif call.data.startswith('achievement_user_remove_achievement_'):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        try:
            target_user_id = int(call.data.rsplit('_', 1)[-1])

            # Получаем achievement_id из кэша
            cache_key = f"remove_achievement_{call.from_user.id}"
            if cache_key in broadcast_cache:
                achievement_id = broadcast_cache[cache_key]

                # Запрашиваем причину
                msg = bot.send_message(
                    call.message.chat.id,
                    f"🗑️ <b>Снятие достижения</b>\n\n"
                    f"<b>Достижение:</b> {get_achievement_emoji(achievement_id)} {achievement_id}\n"
                    f"<b>Пользователь ID:</b> {target_user_id}\n\n"
                    f"Введите причину снятия (или '-' чтобы пропустить):",
                    parse_mode='HTML'
                )
                bot.register_next_step_handler(msg, process_remove_achievement_reason,
                                               target_user_id, achievement_id, call.message.chat.id)

                del broadcast_cache[cache_key]
            else:
                bot.answer_callback_query(call.id, "❌ Ошибка: достижение не выбрано")
        except ValueError as e:
            bot.answer_callback_query(call.id, f"❌ Ошибка формата: {e}")

    #рейтинг
    elif call.data == 'city_rating':
        show_city_rating(call.message.chat.id, call.message.message_id)
    elif call.data == 'admin_city_stats':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        show_city_stats_for_admin(call.message.chat.id)
    # Правила и контент-план
    elif call.data == 'show_rules':
        rules_text = get_rules()
        bot.edit_message_text(
            rules_text,
            chat_id,
            call.message.message_id,
            parse_mode='HTML',
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton('🔙 Назад', callback_data='personal_cabinet')
            )
        )

    elif call.data == 'show_content_plan':
        content_plan_info = get_content_plan_info()

        if content_plan_info['file_id']:
            # Отправляем изображение с подписью
            bot.send_photo(
                chat_id,
                content_plan_info['file_id'],
                caption=content_plan_info['caption'],
                parse_mode='HTML'
            )

            # Показываем кнопку "Назад" в отдельном сообщении
            bot.send_message(
                chat_id,
                "⬇️ <b>Контент-план выше</b>",
                parse_mode='HTML',
                reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton('🔙 Назад', callback_data='personal_cabinet')
                )
            )

            # Удаляем предыдущее сообщение с личным кабинетом
            bot.delete_message(chat_id, call.message.message_id)
        else:
            bot.edit_message_text(
                "📅 <b>Контент-план</b>\n\n"
                "Контент-план ещё не загружен администратором.",
                chat_id,
                call.message.message_id,
                parse_mode='HTML',
                reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton('🔙 Назад', callback_data='personal_cabinet')
                )
            )

    # Мои задачи (по муниципалитету)
    elif call.data == 'my_city_tasks':
        show_user_tasks_by_city(call.from_user.id, call.message.chat.id, message_id=call.message.message_id)

        # Список всех задач
    elif call.data == 'all_tasks_list':
        show_all_tasks(call.message.chat.id, message_id=call.message.message_id)

        # Пагинация для моих задач (по муниципалитету)
    elif call.data.startswith('city_tasks_page_'):
        try:
            page = int(call.data.replace('city_tasks_page_', ''))
            show_user_tasks_by_city(call.from_user.id, call.message.chat.id, page, call.message.message_id)
        except ValueError:
            bot.answer_callback_query(call.id, "❌ Ошибка пагинации")

        # Пагинация для всех задач
    elif call.data.startswith('all_tasks_page_'):
        try:
            page = int(call.data.replace('all_tasks_page_', ''))
            show_all_tasks(call.message.chat.id, page, call.message.message_id)
        except ValueError:
            bot.answer_callback_query(call.id, "❌ Ошибка пагинации")

        # Детальный просмотр задачи из моего муниципалитета
    elif call.data.startswith('show_city_task_detail_'):
        try:
            # Формат: show_city_task_detail_индекс_страница
            parts = call.data.replace('show_city_task_detail_', '').split('_')
            task_index = int(parts[0])  # Это уже индекс в отфильтрованном списке
            page_context = int(parts[1]) if len(parts) > 1 else 0

            show_task_detail_by_city(
                call.from_user.id,
                call.message.chat.id,
                task_index,  # ← Правильный индекс
                page_context,
                call.message.message_id
            )
        except ValueError:
            bot.answer_callback_query(call.id, "❌ Ошибка загрузки задачи")

        # Детальный просмотр задачи из общего списка
    elif call.data.startswith('show_all_task_detail_'):
        try:
            task_index = int(call.data.replace('show_all_task_detail_', ''))

            # Определяем страницу, на которой была задача
            page_context = task_index // TASKS_PER_PAGE

            show_task_detail_all(
                call.message.chat.id,
                task_index,
                page_context,
                call.message.message_id
            )
        except ValueError:
            bot.answer_callback_query(call.id, "❌ Ошибка загрузки задачи")
    # Панель управления задачами
    elif call.data == 'admin_city_tasks':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        show_city_admin_tasks(call.message.chat.id, call.message.message_id)  # ← функция обновлена

    # Добавить задачу
    elif call.data == 'admin_add_task_city':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        start_add_city_task_dialog(call.message.chat.id)

    elif call.data.startswith('raspush_my_tasks_'):
        """Показать активные задачи распуша в разделе Мои задачи"""
        user_id = call.from_user.id
        user = get_user_info(user_id)

        if not user or user['city'] == 'Не указан':
            bot.answer_callback_query(call.id, "❌ Сначала выберите муниципалитет")
            return

        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем активные задачи распуша
        cursor.execute('''
            SELECT id, task_name, task_description, expires_at
            FROM raspush_tasks 
            WHERE expires_at > datetime('now')
            ORDER BY created_at DESC
        ''')

        tasks = cursor.fetchall()

        if not tasks:
            bot.edit_message_text(
                "📭 <b>Активных задач РАСПУШ нет</b>",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='HTML',
                reply_markup=types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton('🔙 Назад', callback_data='my_city_tasks')
                )
            )
            return

        markup = types.InlineKeyboardMarkup()
        for task in tasks[:5]:  # Показываем 5 последних
            # Проверяем, выполнял ли уже этот муниципалитет
            cursor.execute('''
                SELECT 1 FROM raspush_completions 
                WHERE task_id = ? AND city = ?
            ''', (task['id'], user['city']))

            already_completed = cursor.fetchone()

            if not already_completed:
                markup.add(
                    types.InlineKeyboardButton(
                        f"🚀 {task['task_name'][:30]}",
                        callback_data=f"raspush_start_{task['id']}"
                    )
                )

        markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='my_city_tasks'))

        bot.edit_message_text(
            "🚀 <b>Активные задачи РАСПУШ</b>\n\n"
            "Выберите задачу для выполнения:",
            call.message.chat.id,
            call.message.message_id,
            parse_mode='HTML',
            reply_markup=markup
        )

    elif call.data.startswith('accept_task:'):

        task_uid = call.data.split(':', 1)[1]

        user_id = call.from_user.id

        success, message = accept_task_by_uid(task_uid, user_id)

        bot.answer_callback_query(call.id)

        if success:

            bot.edit_message_text(

                message,

                call.message.chat.id,

                call.message.message_id,

                parse_mode='HTML'

            )

        else:

            bot.send_message(call.message.chat.id, message)



    # Кнопка "Снять задачу" в админ-панели
    elif call.data == 'admin_complete_task_menu':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        show_complete_task_menu(call.message.chat.id)

    elif call.data == 'admin_clear_responsible_menu':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        show_clear_responsible_menu(call.message.chat.id)

    elif call.data.startswith('complete_task_'):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        task_index = int(call.data.replace('complete_task_', ''))

        # Запрашиваем ID пользователя
        msg = bot.send_message(
            call.message.chat.id,
            "👤 <b>Отметить задачу выполненной</b>\n\n"
            "Введите ID пользователя, который выполнил задачу:",
            parse_mode='HTML'
        )

        def process_user_for_completion(message):
            try:
                user_id = int(message.text)

                # Запрашиваем баллы
                msg2 = bot.send_message(
                    message.chat.id,
                    "💰 Введите количество баллов для начисления (0 если не нужно):",
                    parse_mode='HTML'
                )

                def process_points_for_completion(msg2):
                    try:
                        points = int(msg2.text)
                        if points < 0:
                            bot.send_message(msg2.chat.id, "❌ Количество баллов не может быть отрицательным")
                            return

                        # Запрашиваем причину
                        msg3 = bot.send_message(
                            msg2.chat.id,
                            "📝 Введите причину выполнения (или '-' для пропуска):",
                            parse_mode='HTML'
                        )

                        def process_reason_for_completion(msg3):
                            reason = msg3.text.strip()
                            if reason == '-':
                                reason = ""

                            # Выполняем завершение задачи
                            success, result = complete_task_with_points(
                                task_index, user_id, points, reason
                            )

                            if success:
                                # Получаем информацию о пользователе для красивого ответа
                                user_info = get_user_info(user_id)
                                if user_info:
                                    city_emoji = AVAILABLE_CITIES.get(user_info['city'], '🏙️')
                                    bot.send_message(
                                        msg3.chat.id,
                                        f"✅ <b>Задача отмечена выполненной!</b>\n\n"
                                        f"<b>Исполнитель:</b> {user_info['first_name']} ({city_emoji} {user_info['city']})\n"
                                        f"<b>Баллы:</b> {'+' + str(points) if points > 0 else '0'}\n"
                                        f"<b>Причина:</b> {reason if reason else 'не указана'}\n\n"
                                        f"{result}",
                                        parse_mode='HTML'
                                    )
                                else:
                                    bot.send_message(msg3.chat.id, result, parse_mode='HTML')
                            else:
                                bot.send_message(msg3.chat.id, result, parse_mode='HTML')

                            # Возвращаем к списку задач
                            show_city_admin_tasks(msg3.chat.id)

                        bot.register_next_step_handler(msg3, process_reason_for_completion)

                    except ValueError:
                        bot.send_message(msg2.chat.id, "❌ Введите число")

                bot.register_next_step_handler(msg2, process_points_for_completion)

            except ValueError:
                bot.send_message(message.chat.id, "❌ Введите числовой ID пользователя")

        bot.register_next_step_handler(msg, process_user_for_completion)
    # ДОБАВИТЬ этот обработчик в callback_handler:
    elif call.data == 'admin_delete_raspush_menu':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, task_name, created_at 
            FROM raspush_tasks 
            ORDER BY created_at DESC 
            LIMIT 10
        ''')

        tasks = cursor.fetchall()

        if not tasks:
            bot.send_message(
                call.message.chat.id,
                "📭 <b>Нет активных задач РАСПУШ</b>",
                parse_mode='HTML'
            )
            return

        markup = types.InlineKeyboardMarkup()
        for task in tasks:
            date = datetime.strptime(task['created_at'], "%Y-%m-%d %H:%M:%S").strftime("%d.%m")
            markup.add(
                types.InlineKeyboardButton(
                    f"#{task['id']} {task['task_name'][:20]} ({date})",
                    callback_data=f"confirm_delete_raspush_{task['id']}"
                )
            )

        markup.add(types.InlineKeyboardButton('🔙 Назад', callback_data='admin_city_tasks'))

        bot.edit_message_text(
            "🗑️ <b>Выберите задачу РАСПУШ для удаления:</b>",
            call.message.chat.id,
            call.message.message_id,
            parse_mode='HTML',
            reply_markup=markup
        )

    elif call.data.startswith('confirm_delete_raspush_'):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        task_id = int(call.data.split('_')[-1])

        # Запрашиваем подтверждение
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton('✅ Да, удалить', callback_data=f'execute_delete_raspush_{task_id}'),
            types.InlineKeyboardButton('❌ Отмена', callback_data='admin_city_tasks')
        )

        bot.edit_message_text(
            f"⚠️ <b>Вы уверены, что хотите удалить задачу #{task_id}?</b>\n\n"
            f"Все отчеты по этой задаче также будут удалены.",
            call.message.chat.id,
            call.message.message_id,
            parse_mode='HTML',
            reply_markup=markup
        )

    elif call.data.startswith('execute_delete_raspush_'):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        task_id = int(call.data.split('_')[-1])
        success, message = delete_raspush_task(task_id, call.from_user.id)

        bot.answer_callback_query(call.id, "✅ Удалено" if success else "❌ Ошибка")
        bot.send_message(call.message.chat.id, message, parse_mode='HTML')

        # Возвращаем к списку задач
        show_city_admin_tasks(call.message.chat.id)

    elif call.data.startswith('clear_responsible_'):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        task_index = int(call.data.replace('clear_responsible_', ''))

        # Просто очищаем ответственного
        success, result = clear_task_responsible(task_index)

        if success:
            bot.answer_callback_query(call.id, "✅ Ответственный очищен")
            bot.send_message(
                call.message.chat.id,
                result,
                parse_mode='HTML'
            )
        else:
            bot.answer_callback_query(call.id, "❌ Ошибка")
            bot.send_message(
                call.message.chat.id,
                result,
                parse_mode='HTML'
            )

        # Возвращаем к списку задач
        show_city_admin_tasks(call.message.chat.id)

    # Выбор муниципалитета для задачи
    elif call.data.startswith('select_task_city_'):
        city = call.data.split('_')[-1]
        process_task_city_selection(call, city)

    elif call.data == 'task_back_to_deadline':
        # Возврат к выбору срока
        process_task_city_selection(call, broadcast_cache.get(f"task_city_{call.from_user.id}"))

    # Выбор награды
    elif call.data.startswith('task_points_'):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        points = call.data.replace('task_points_', '')
        process_task_points_selection(call, points)

    # Статистика задач
    elif call.data == 'admin_tasks_stats':
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
                SELECT 
                    COUNT(*) as total_tasks,
                    SUM(CASE WHEN is_completed = 0 THEN 1 ELSE 0 END) as active_tasks,
                    SUM(CASE WHEN is_completed = 1 THEN 1 ELSE 0 END) as completed_tasks,
                    COUNT(DISTINCT assigned_city) as cities_count,
                    SUM(points_reward) as total_points
                FROM bot_tasks
            ''')

        stats = cursor.fetchone()

        cursor.execute('''
                SELECT assigned_city, COUNT(*) as task_count
                FROM bot_tasks
                WHERE is_completed = 0
                GROUP BY assigned_city
                ORDER BY task_count DESC
                LIMIT 5
            ''')

        top_cities = cursor.fetchall()

        response = (
            "📊 <b>Статистика задач</b>\n\n"
            f"<b>Всего задач:</b> {stats['total_tasks']}\n"
            f"<b>Активные:</b> {stats['active_tasks']}\n"
            f"<b>Выполненные:</b> {stats['completed_tasks']}\n"
            f"<b>Муниципалитетов с задачами:</b> {stats['cities_count']}\n"
            f"<b>Всего баллов к начислению:</b> 🏅 {stats['total_points'] or 0}\n\n"
        )

        if top_cities:
            response += "<b>🏆 Топ муниципалитетов по активным задачам:</b>\n"
            for city in top_cities:
                city_emoji = AVAILABLE_CITIES.get(city['assigned_city'], '🏙️')
                response += f"• {city_emoji} {city['assigned_city']}: {city['task_count']} задач\n"

        bot.edit_message_text(
            response,
            call.message.chat.id,
            call.message.message_id,
            parse_mode='HTML',
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton('🔙 Назад', callback_data='admin_city_tasks')
            )
        )
    # Рассылка
    elif call.data == 'admin_broadcast':
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        bot.delete_message(chat_id, call.message.message_id)
        show_broadcast_options(chat_id)

    elif call.data == 'broadcast_all':
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        bot.delete_message(chat_id, call.message.message_id)
        ask_for_broadcast_text(chat_id, 'all', 'all')

    elif call.data == 'broadcast_by_city':
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        bot.delete_message(chat_id, call.message.message_id)
        show_cities_for_broadcast(chat_id)

    elif call.data.startswith('broadcast_city_'):
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return
        city = call.data.replace('broadcast_city_', '')
        bot.delete_message(chat_id, call.message.message_id)
        ask_for_broadcast_text(chat_id, 'city', city)

    elif call.data.startswith('confirm_broadcast_'):
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        cache_key = call.data.replace('confirm_broadcast_', '')
        if cache_key not in broadcast_cache:
            bot.answer_callback_query(call.id, "❌ Текст не найден")
            return

        broadcast_data = broadcast_cache[cache_key]
        parts = cache_key.split('_')
        target_type, target_value = parts[1], '_'.join(parts[2:])

        bot.delete_message(chat_id, call.message.message_id)
        bot.send_message(chat_id, "⏳ <b>Рассылка запущена...</b>", parse_mode='HTML')

        send_broadcast(chat_id, target_type, target_value, broadcast_data, user_id)

        if cache_key in broadcast_cache:
            del broadcast_cache[cache_key]

    # Список пользователей
    elif call.data == 'admin_list_users':
        if not is_admin_user:
            bot.answer_callback_query(call.id, "⛔ Нет доступа")
            return

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT user_id, first_name, city, points FROM users ORDER BY points DESC LIMIT 20')
        users = cursor.fetchall()

        response = "<b>📊 Список пользователей:</b>\n\n"
        for i, user in enumerate(users, 1):
            city_emoji = AVAILABLE_CITIES.get(user['city'], '🏙️')
            response += f"{i}. {user['user_id']} | {user['first_name']} | {city_emoji} {user['city']} | {user['points']} баллов\n"

        bot.edit_message_text(
            response,
            chat_id,
            call.message.message_id,
            parse_mode='HTML',
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton('🔙 Назад', callback_data='admin_panel')
            )
        )

    # Топ пользователей
    elif call.data == 'top_users':
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT first_name, city, points FROM users ORDER BY points DESC LIMIT 10')
        top_users = cursor.fetchall()

        response = "<b>🏆 Топ-10:</b>\n\n"
        for i, user in enumerate(top_users, 1):
            city_emoji = AVAILABLE_CITIES.get(user['city'], '🏙️')
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            response += f"{medal} {user['first_name']} ({city_emoji} {user['city']}): {user['points']} баллов\n"

        markup = types.InlineKeyboardMarkup()
        if is_admin_user:
            markup.add(types.InlineKeyboardButton('⚙️ Админ-панель', callback_data='admin_panel'))

        bot.edit_message_text(response, chat_id, call.message.message_id, parse_mode='HTML', reply_markup=markup)

    # Выход из админ-панели
    elif call.data == 'exit_admin':
        bot.edit_message_text(
            "✅ Вы вышли из админ-панели",
            chat_id,
            call.message.message_id,
            reply_markup=types.InlineKeyboardMarkup().add(
                types.InlineKeyboardButton('👤 Личный кабинет', callback_data='personal_cabinet')
            )
        )


@bot.callback_query_handler(func=lambda call: call.data == "admin_create_raspush")
def admin_create_raspush_handler(call):
    if not is_admin(call.from_user.id):
        return

    msg = bot.send_message(
        call.message.chat.id,
        "🚀 <b>Создание задачи РАСПУШ</b>\n\n"
        "Введите название задачи:",
        parse_mode="HTML"
    )

    bot.register_next_step_handler(msg, process_raspush_name)

# ==============================
# 8. ЗАПУСК БОТА
# ==============================
if __name__ == '__main__':
    print("Запуск бота...")

    try:
        # 1. Создаем базовые таблицы
        init_db()

        # 2. Проверяем все таблицы
        ensure_tables_exist()

        print("База данных готова к работе")

        # Запускаем проверку дедлайнов в отдельном потоке
        deadline_thread = threading.Thread(target=check_task_deadlines, daemon=True)
        deadline_thread.start()


        # ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ОШИБОК ДЛЯ ОПРОСА
        def polling_with_error_handling():
            while True:
                try:
                    bot.polling(none_stop=True, interval=0, timeout=30, long_polling_timeout=30)
                except Exception as e:
                    error_msg = f"Критическая ошибка polling: {str(e)}\n\n{traceback.format_exc()}"
                    print(f"Ошибка: {error_msg}")
                    from config import send_error_to_admin
                    send_error_to_admin(error_msg)
                    time.sleep(10)  # Пауза перед перезапуском


        # Запускаем опрос с обработкой ошибок
        polling_thread = threading.Thread(target=polling_with_error_handling, daemon=True)
        polling_thread.start()

        # Держим основной поток активным
        polling_thread.join()

    except Exception as e:
        error_msg = f"Ошибка при запуске: {str(e)}\n\n{traceback.format_exc()}"
        print(error_msg)
        from config import send_error_to_admin

        send_error_to_admin(error_msg)