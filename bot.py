import os
import re
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import vk_api
from vk_api.utils import get_random_id
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool as psycopg2_pool
from datetime import datetime

import requests

# --- ЛОГИРОВАНИЕ (print для гарантированного вывода в лог) ---
def log_msg(msg):
    print(f"[BOT] {msg}", flush=True)

# --- РЕГЕКСЫ ---
PHONE_PATTERN = re.compile(r'^(\+7|7|8)?[\s\-]?$?\d{3}$?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}$')
EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')
NAME_PATTERN = re.compile(r"^[а-яёА-ЯЁa-zA-Z]+(?:['\-][а-яёА-ЯЁa-zA-Z]+)*$")

# --- ПРЕДВЫЧИСЛЕННЫЕ КЛАВИАТУРЫ ---
KB_START = json.dumps({"one_time": False, "buttons": [[{"action": {"type": "text", "label": "Начать анкету"}, "color": "positive"}]})
KB_RESTART = json.dumps({"one_time": False, "buttons": [[{"action": {"type": "text", "label": "🔄 Пройти заново"}, "color": "negative"}]})

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def format_numbered_list(items, start_from=1, truncate=True):
    lines = []
    for i, item in enumerate(items, start=start_from):
        if truncate and len(item) > 80:
            item = item[:77] + "…"
        lines.append(f"{i} — {item}")
    return "\n".join(lines)

def validate_fio(text):
    text = text.strip()
    lower = text.lower()
    if lower in ("начать анкету", "пройти заново", "🔄 пройти заново", "/restart"):
        return False, (
            "Кажется, вы нажали кнопку вместо ответа 🙂\n"
            "Пожалуйста, укажите Фамилию Имя и Отчество через пробел.\n"
            "Например: Иванов Иван Иванович\n"
            "Если отчества нет — поставьте «-» (например: Иванов Иван -)."
        )
    parts = text.split()
    if len(parts) != 3:
        return False, (
            "Нужно указать ровно три слова: Фамилия, Имя, Отчество — через пробел.\n"
            "Например: Иванов Иван Иванович\n"
            "Если отчества нет — поставьте «-» (например: Иванов Иван -)."
        )
    surname, first_name, patronymic = parts
    errors = []
    if not NAME_PATTERN.match(surname) or len(surname) < 2:
        errors.append("фамилия")
    if not NAME_PATTERN.match(first_name) or len(first_name) < 2:
        errors.append("имя")
    if patronymic != "-" and (not NAME_PATTERN.match(patronymic) or len(patronymic) < 2):
        errors.append("отчество")
    if errors:
        hint = (
            "Проверьте следующие поля: " + ", ".join(errors) + ".\n"
            "Используйте только буквы (допускается дефис в двойных фамилиях).\n"
            "Если отчества нет — поставьте «-» (например: Иванов Иван -)."
        )
        return False, hint

    def cap(s):
        if s == "-":
            return s
        return "-".join(p.capitalize() for p in s.split("-"))

    normalized = f"{cap(surname)} {cap(first_name)} {cap(patronymic)}"
    return True, normalized

def validate_contacts(text):
    text = text.strip()
    parts = re.split(r'[\s,;]+', text)
    phone = None
    email = None
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if phone is None and PHONE_PATTERN.match(part):
            digits = re.sub(r'\D', '', part)
            if len(digits) == 11 and digits.startswith('8'):
                digits = '7' + digits[1:]
            elif len(digits) == 10:
                digits = '7' + digits
            if len(digits) == 11:
                phone = '+7' + digits[-10:]
        elif email is None and EMAIL_PATTERN.match(part):
            email = part.lower()
    if phone or email:
        contacts = []
        if phone:
            contacts.append(phone)
        if email:
            contacts.append(email)
        return True, "; ".join(contacts)
    return False, None

# ================= НАСТРОЙКИ ИЗ ENV =================
VK_TOKEN = os.getenv("VK_TOKEN")
ADMIN_IDS = set(map(int, os.getenv("ADMIN_IDS", "").split(","))) if os.getenv("ADMIN_IDS") else set()
GROUP_ID = int(os.getenv("GROUP_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")
CALLBACK_CONFIRM_TOKEN = os.getenv("CALLBACK_CONFIRM_TOKEN", "")

if not VK_TOKEN or not DATABASE_URL:
    raise ValueError("Не заданы переменные окружения VK_TOKEN или DATABASE_URL")
# =====================================================

# ----------------- ПУЛ СОЕДИНЕНИЙ БД -----------------
db_pool = None

def init_db_pool():
    global db_pool
    # Добавляем таймауты в DSN
    dsn = DATABASE_URL + " connect_timeout=5"
    db_pool = psycopg2_pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=30,
        dsn=dsn
    )

def get_db():
    conn = db_pool.getconn()
    # Устанавливаем statement_timeout на уровне сессии
    cur = conn.cursor()
    cur.execute("SET statement_timeout = 10000")
    cur.close()
    return conn

def release_db(conn):
    db_pool.putconn(conn)

def init_db():
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS answers (
                user_id BIGINT PRIMARY KEY,
                fio TEXT, institution TEXT, specialty TEXT, study_group TEXT,
                course TEXT, form_of_study TEXT, contacts TEXT,
                employment_status TEXT, target_contract TEXT, experience TEXT,
                practice_eval TEXT, events TEXT, resume_status TEXT,
                interview_training TEXT, special_status TEXT, military TEXT,
                maternity TEXT, graduate TEXT, post_plans TEXT, help_needed TEXT)""")
        c.execute("ALTER TABLE answers ADD COLUMN IF NOT EXISTS consent_status BOOLEAN")
        c.execute("ALTER TABLE answers ADD COLUMN IF NOT EXISTS created_at TIMESTAMP")
        c.execute("""CREATE TABLE IF NOT EXISTS progress (
                user_id BIGINT PRIMARY KEY,
                step_index INTEGER DEFAULT 0,
                uni_page INTEGER DEFAULT 0,
                started INTEGER DEFAULT 0)""")
        c.execute("ALTER TABLE progress ADD COLUMN IF NOT EXISTS started_at TIMESTAMP")
        c.execute("CREATE INDEX IF NOT EXISTS idx_progress_user_id ON progress(user_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_answers_user_id ON answers(user_id)")
        conn.commit()
    except Exception as e:
        conn.rollback()
        log_msg(f"DB init error: {e}")
        raise
    finally:
        release_db(conn)

# ----------------- КЭШ ПРОГРЕССА В ПАМЯТИ -----------------
# Это убирает лишние SELECT на каждый шаг
progress_cache = {}  # user_id -> {step_index, uni_page, started}
cache_lock = threading.Lock()

def load_progress_to_cache(user_id):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT step_index, uni_page, started FROM progress WHERE user_id=%s", (user_id,))
        row = c.fetchone()
        with cache_lock:
            if row:
                progress_cache[user_id] = {
                    "step_index": row[0] if row[0] is not None else 0,
                    "uni_page": row[1] if row[1] is not None else 0,
                    "started": row[2] if row[2] is not None else 0
                }
            else:
                progress_cache[user_id] = {"step_index": 0, "uni_page": 0, "started": 0}
    except Exception as e:
        log_msg(f"Error loading progress to cache: {e}")
    finally:
        release_db(conn)

def get_progress_cached(user_id):
    with cache_lock:
        if user_id not in progress_cache:
            load_progress_to_cache(user_id)
        return progress_cache[user_id]["step_index"], progress_cache[user_id]["uni_page"], progress_cache[user_id]["started"]

def set_progress_cached(user_id, step_index, uni_page=0, started=1):
    # Сначала обновляем в БД
    conn = get_db()
    try:
        c = conn.cursor()
        if started == 1:
            c.execute(
                "INSERT INTO progress (user_id, step_index, uni_page, started, started_at) "
                "VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT (user_id) DO UPDATE SET step_index=%s, uni_page=%s, started=%s, "
                "started_at=COALESCE(progress.started_at, EXCLUDED.started_at)",
                (user_id, step_index, uni_page, started, datetime.now(),
                 step_index, uni_page, started)
            )
        else:
            c.execute(
                "INSERT INTO progress (user_id, step_index, uni_page, started) "
                "VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (user_id) DO UPDATE SET step_index=%s, uni_page=%s, started=%s",
                (user_id, step_index, uni_page, started, step_index, uni_page, started)
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        log_msg(f"Error setting progress: {e}")
        raise
    finally:
        release_db(conn)
    
    # Потом обновляем кэш
    with cache_lock:
        if user_id not in progress_cache:
            progress_cache[user_id] = {}
        progress_cache[user_id]["step_index"] = step_index
        progress_cache[user_id]["uni_page"] = uni_page
        progress_cache[user_id]["started"] = started

def save_answer_db(user_id, field, value):
    cols = ["fio","institution","specialty","study_group","course","form_of_study",
            "contacts","employment_status","target_contract","experience",
            "practice_eval","events","resume_status","interview_training",
            "special_status","military","maternity","graduate","post_plans","help_needed"]
    if field not in cols:
        return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(
            f"INSERT INTO answers (user_id, {field}) VALUES (%s, %s) "
            f"ON CONFLICT (user_id) DO UPDATE SET {field}=EXCLUDED.{field}",
            (user_id, value)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        log_msg(f"Error saving answer: {e}")
        raise
    finally:
        release_db(conn)

# ----------------- VK С ТАЙМАУТОМ -----------------
vk_session = vk_api.VkApi(token=VK_TOKEN)
vk = vk_session.get_api()

# Исправленная строка: vk_session.http.request вместо .session.request
_original_request = vk_session.http.request

def _patched_request(method, url, **kwargs):
    kwargs.setdefault("timeout", 5)
    return _original_request(method, url, **kwargs)

vk_session.http.request = _patched_request

def send_message(user_id, message, keyboard=None, attachment=None):
    try:
        params = {"peer_id": user_id, "message": message, "random_id": get_random_id()}
        if keyboard:
            params["keyboard"] = keyboard
        if attachment:
            params["attachment"] = attachment
        vk.messages.send(**params)
    except Exception as e:
        log_msg(f"VK send error: {e}")

# ----------------- ШАГИ АНКЕТЫ -----------------
STEPS = [
    "welcome",
    "fio",
    "consent",
    "institution",
    "specialty",
    "study_group",
    "course",
    "form_of_study",
    "contacts",
    "employment_status",
    "target_contract",
    "experience",
    "practice_eval",
    "events",
    "resume_status",
    "interview_training",
    "special_status",
    "military",
    "maternity",
    "graduate",
    "post_plans",
    "help_needed",
    "finish"
]

STEP_TO_DB = {
    "fio": "fio",
    "institution": "institution",
    "specialty": "specialty",
    "study_group": "study_group",
    "course": "course",
    "form_of_study": "form_of_study",
    "contacts": "contacts",
    "employment_status": "employment_status",
    "target_contract": "target_contract",
    "experience": "experience",
    "practice_eval": "practice_eval",
    "events": "events",
    "resume_status": "resume_status",
    "interview_training": "interview_training",
    "special_status": "special_status",
    "military": "military",
    "maternity": "maternity",
    "graduate": "graduate",
    "post_plans": "post_plans",
    "help_needed": "help_needed"
}

UNIVERSITIES = [
    "БПОУ УР «Воткинский промышленный техникум»",
    "БПОУ УР «Воткинский музыкально-педагогический колледж имени П.И. Чайковского»",
    "БПОУ УР «Воткинский машиностроительный техникум имени В.Г.Садовникова»",
    "АПОУ УР «Глазовский аграрно-промышленный техникум»",
    "БПОУ УР «Глазовский технический колледж»",
    "БПОУ «Дебесский политехникум»",
    "Филиал БПОУ УР «Дебесский политехникум» п. Кез",
    "БПОУР «Игринский политехнический техникум»",
    "БПОУ УР «Ижевский торгово-экономический техникум»",
    "БПОУ УР «Ижевский монтажный техникум»",
    "БПОУ «Ижевский агростроительный техникум»",
    "ЧПОО «Нефтяной техникум»",
    "БПОУ УР «Ижевский политехнический колледж»",
    "БПОУ УР «Ижевский промышленно-экономический колледж»",
    "Филиал АПОУ УР Ижевский промышленно-экономический колледж в г.Можга",
    "БПОУ УР «Ижевский машиностроительный техникум им. С.Н. Борина»",
    "БПОУ УР «Радиомеханический техникум имени В.А. Шутова»",
    "АПОУ УР «Экономико-технологический колледж»",
    "БПОУ УР «Асановский аграрно-технический техникум»",
    "АПОУ УР «Топливно-энергетический колледж»",
    "БПОУ УР «Ижевский техникум индустрии питания»",
        "КПОУ УР «Удмуртский республиканский колледж культуры и искусства»",
    "БПОУ УР «Сарапульский индустриальный техникум»",
    "АПОУ УР «Сарапульский политехнический колледж»",
    "БПОУ УР «Можгинский агропромышленный колледж»",
    "БПОУ УР «Увинский профессиональный колледж»",
    "БПОУ УР «Якшур-Бодьинский политехникум»"
]

DISTRICTS_UNIVERSITIES = {
    "Ижевск": [u for u in UNIVERSITIES if "Ижевский" in u or "филиал" in u.lower()],
    "Воткинск": [u for u in UNIVERSITIES if "Воткинский" in u],
    "Глазов": [u for u in UNIVERSITIES if "Глазовский" in u],
    "Можга": [u for u in UNIVERSITIES if "Можга" in u.lower() or "филиал" in u.lower()],
    "Сарапул": [u for u in UNIVERSITIES if "Сарапульский" in u],
    "Ува": [u for u in UNIVERSITIES if "Увинский" in u],
    "Дебесы": [u for u in UNIVERSITIES if "Дебесский" in u],
    "Якшур‑Бодья": [u for u in UNIVERSITIES if "Якшур-Бодьинский" in u],
    "Игрино": [u for u in UNIVERSITIES if "Игринский" in u],
    "Асаново": [u for u in UNIVERSITIES if "Асановский" in u],
    "Кез": [u for u in UNIVERSITIES if "Кез" in u],
}

# ----------------- ЛОГИКА ШАГОВ -----------------
def ask_step(user_id, step_name):
    if step_name == "welcome":
        msg = (
            "👋 Привет! Это бот для сбора данных для центра карьеры.\n"
            "Пройдём короткую анкету — это займёт 3–5 минут.\n\n"
            "Нажмите кнопку «Начать анкету», чтобы продолжить."
        )
        send_message(user_id, msg, keyboard=KB_START)
    elif step_name == "fio":
        msg = (
            "📝 Пожалуйста, укажите Фамилию, Имя и Отчество через пробел.\n"
            "Например: Иванов Иван Иванович\n"
            "Если отчества нет — поставьте «-» (например: Иванов Иван -)."
        )
        send_message(user_id, msg)
    elif step_name == "consent":
        msg = (
            "✅ Вы согласны на обработку персональных данных?\n"
            "Пожалуйста, ответьте «Да» или «Нет»."
        )
        send_message(user_id, msg)
    elif step_name == "institution":
        districts = list(DISTRICTS_UNIVERSITIES.keys())
        msg = "📍 Выберите ваш населённый пункт (или напишите название):\n" + format_numbered_list(districts)
        send_message(user_id, msg)
    elif step_name == "specialty":
        # Здесь можно подставить конкретный список специальностей по выбранному вузу, пока — заглушка
        msg = (
            "🎓 Напишите направление/специальность, на которой вы учитесь.\n"
            "Можно кратко, как в зачётке."
        )
        send_message(user_id, msg)
    elif step_name == "study_group":
        msg = "📋 Укажите номер вашей учебной группы (например: ИВТ-231)."
        send_message(user_id, msg)
    elif step_name == "course":
        msg = "🎓 Укажите курс (цифрой): 1, 2, 3, 4 или 5."
        send_message(user_id, msg)
    elif step_name == "form_of_study":
        msg = (
            "📚 Форма обучения:\n"
            "1 — Очная\n"
            "2 — Очно‑заочная\n"
            "3 — Заочная"
        )
        send_message(user_id, msg)
    elif step_name == "contacts":
        msg = (
            "📞 Укажите контакты для связи: телефон и/или email.\n"
            "Можно в любом порядке, через пробел или запятую."
        )
        send_message(user_id, msg)
    elif step_name == "employment_status":
        msg = (
            "💼 Ваш текущий статус занятости:\n"
            "1 — Работаю\n"
            "2 — Не работаю\n"
            "3 — В декрете\n"
            "4 — Другое"
        )
        send_message(user_id, msg)
    elif step_name == "target_contract":
        msg = (
            "📄 Планируете ли вы целевое обучение/трудоустройство?\n"
            "Ответьте «Да» или «Нет», или кратко опишите планы."
        )
        send_message(user_id, msg)
    elif step_name == "experience":
        msg = (
            "🧑‍💼 Есть ли у вас опыт работы по специальности?\n"
            "Напишите кратко: где и сколько месяцев/лет."
        )
        send_message(user_id, msg)
    elif step_name == "practice_eval":
        msg = (
            "🏫 Как вы оцениваете свою практику?\n"
            "Кратко: что понравилось, что хотелось бы улучшить."
        )
        send_message(user_id, msg)
    elif step_name == "events":
        msg = (
            "🗓 Участвовали ли вы в карьерных мероприятиях центра?\n"
            "Напишите, какие, или «Нет» — если не участвовали."
        )
        send_message(user_id, msg)
    elif step_name == "resume_status":
        msg = (
            "📄 Есть ли у вас резюме?\n"
            "«Да» / «Нет» / «Есть, но не обновлено»"
        )
        send_message(user_id, msg)
    elif step_name == "interview_training":
        msg = (
            "🗣 Хотели бы пройти подготовку к собеседованию?\n"
            "«Да» / «Нет»"
        )
        send_message(user_id, msg)
    elif step_name == "special_status":
        msg = (
            "⚠️ Есть ли особые обстоятельства (инвалидность, ОВЗ, иные статусы)?\n"
            "Напишите «Нет» или кратко укажите статус."
        )
        send_message(user_id, msg)
    elif step_name == "military":
        msg = (
            "🪖 Воинская обязанность:\n"
            "«Призывник» / «В запасе» / «Не подлежит» / «Другое»"
        )
        send_message(user_id, msg)
    elif step_name == "maternity":
        msg = (
            "👩‍🍼 Статус по материнству/отцовству:\n"
            "«В декрете» / «Планирую» / «Не актуально»"
        )
        send_message(user_id, msg)
    elif step_name == "graduate":
        msg = (
            "🎓 Вы выпускник этого года?\n"
            "«Да» / «Нет»"
        )
        send_message(user_id, msg)
    elif step_name == "post_plans":
        msg = (
            "🚀 Ваши планы после выпуска:\n"
            "Работа, магистратура, переезд, другое — кратко."
        )
        send_message(user_id, msg)
    elif step_name == "help_needed":
        msg = (
            "🤝 Какая помощь от центра карьеры вам нужна?\n"
            "Вакансии, стажировки, резюме, профориентация — напишите 1–2 пункта."
        )
        send_message(user_id, msg)
    elif step_name == "finish":
        msg = (
            "🎉 Спасибо за заполнение анкеты!\n"
            "Ваши данные переданы в центр карьеры.\n"
            "При необходимости с вами свяжутся."
        )
        send_message(user_id, msg, keyboard=KB_RESTART)

def get_next_step_index(current_index):
    # Защита от выхода за границы
    next_idx = current_index + 1
    if next_idx >= len(STEPS):
        return len(STEPS) - 1  # последний шаг — finish
    return next_idx

def handle_message(user_id, text):
    t0 = time.time()
    step_index, uni_page, started = get_progress_cached(user_id)
    t_db_read = time.time() - t0

    # Если анкета ещё не начата — проверяем кнопку «Начать»
    if started == 0 and text.strip().lower() in ["начать анкету", "начать"]:
        set_progress_cached(user_id, 0, uni_page=0, started=1)
        step_index = 0
        ask_step(user_id, STEPS[step_index])
        t_total = time.time() - t0
        log_msg(f"handle_message user={user_id} step={STEPS[step_index]} duration={t_total:.3f}s db={t_db_read:.3f}s")
        return

    # Обработка по шагам
    current_step = STEPS[step_index]
    validated_value = None
    error_msg = None

    if current_step == "fio":
        ok, result = validate_fio(text)
        if not ok:
            send_message(user_id, result)
            t_total = time.time() - t0
            log_msg(f"handle_message user={user_id} step={current_step} duration={t_total:.3f}s db={t_db_read:.3f}s")
            return
        validated_value = result
    elif current_step == "consent":
        lower = text.strip().lower()
        if lower in ["да", "yes", "1"]:
            validated_value = True
        elif lower in ["нет", "no", "0"]:
            validated_value = False
        else:
            send_message(user_id, "Пожалуйста, ответьте «Да» или «Нет».")
            t_total = time.time() - t0
            log_msg(f"handle_message user={user_id} step={current_step} duration={t_total:.3f}s db={t_db_read:.3f}s")
            return
    elif current_step == "contacts":
        ok, result = validate_contacts(text)
        if not ok:
            send_message(user_id, "Не удалось распознать контакты. Напишите телефон и/или email через пробел или запятую.")
            t_total = time.time() - t0
            log_msg(f"handle_message user={user_id} step={current_step} duration={t_total:.3f}s db={t_db_read:.3f}s")
            return
        validated_value = result
    elif current_step in ["course", "form_of_study", "employment_status", "military", "maternity", "graduate"]:
        # Для простоты принимаем любой текст, можно добавить валидацию
        validated_value = text.strip()

    # Сохраняем ответ, если есть что сохранять
    if validated_value is not None and current_step in STEP_TO_DB:
        save_answer_db(user_id, STEP_TO_DB[current_step], validated_value)

    # Переходим к следующему шагу
    next_idx = get_next_step_index(step_index)
    set_progress_cached(user_id, next_idx, uni_page, 1)

    # Показываем следующий шаг
    ask_step(user_id, STEPS[next_idx])

    t_total = time.time() - t0
    log_msg(f"handle_message user={user_id} step={current_step} duration={t_total:.3f}s db={t_db_read:.3f}s")

# ----------------- ФОНОВАЯ ОБРАБОТКА НЕПРОЧИТАННЫХ -----------------
def process_unread_messages():
    """Ограниченная фоновая обработка: только последние 10 непрочитанных диалогов"""
    try:
        vk_api_obj = vk_session.get_api()
        resp = vk_api_obj.messages.getConversations(filter="unread", count=10)
        items = resp.get("items", [])
        for item in items:
            peer = item.get("conversation", {}).get("peer", {})
            if not peer:
                continue
            peer_id = peer.get("id")
            # Получаем только последнее сообщение
            history = vk_api_obj.messages.getHistory(peer_id=peer_id, count=1)
            msgs = history.get("items", [])
            if msgs:
                msg = msgs[0]
                text = msg.get("text", "")
                user_id = msg.get("from_id")
                if user_id:
                    # Обрабатываем в отдельном потоке, чтобы не блокировать основной пул
                    executor_heavy.submit(handle_message, user_id, text)
    except Exception as e:
        log_msg(f"Error in process_unread_messages: {e}")

# ----------------- ПУЛЫ ПОТОКОВ -----------------
_executor = ThreadPoolExecutor(max_workers=30)  # обычные ответы
_executor_heavy = ThreadPoolExecutor(max_workers=3)  # фоновые задачи и тяжёлые операции

# ----------------- ОБРАБОТЧИК VK CALLBACK -----------------
def callback_handler(event):
    obj = event.get("object", {})
    message = obj.get("message", {})
    user_id = message.get("from_id")
    text = message.get("text", "").strip()
    if not user_id or not text:
        return {"ok": True}

    # Отправляем в пул для обычных ответов
    _executor.submit(handle_message, user_id, text)
    return {"ok": True}

# ----------------- ЗАПУСК -----------------
if __name__ == "__main__":
    init_db_pool()
    init_db()
    log_msg("DB initialized")

    # Запускаем фоновую обработку раз в 5 минут
    def run_background():
        while True:
            time.sleep(300)
            process_unread_messages()

    threading.Thread(target=run_background, daemon=True).start()
    log_msg("Background unread processor started")

    from fastapi import FastAPI
    app = FastAPI()

    @app.post("/")
    async def vk_callback(event: dict):
        # Это синхронный обработчик, но логика вынесена в потоки
        callback_handler(event)
        return {"ok": True}

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
