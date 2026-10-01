import asyncio
from contextlib import contextmanager
import vk_api
from vk_api.utils import get_random_id
import re
import os
import json
import time
import tempfile
import threading
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool as psycopg2_pool
from datetime import datetime, date
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
import requests
from requests.adapters import HTTPAdapter
import concurrent.futures

# --- ЛОГИРОВАНИЕ ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("vk_bot")

def log_msg(msg):
    print(f"[BOT] {msg}", flush=True)

# --- РЕГЕКСЫ ---
PHONE_PATTERN = re.compile(r'^(\+7|7|8)?[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}$')
EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')
NAME_PATTERN = re.compile(r"^[а-яёА-ЯЁa-zA-Z]+(?:['\-][а-яёА-ЯЁa-zA-Z]+)*$")

# --- ПРЕДВЫЧИСЛЕННЫЕ КЛАВИАТУРЫ ---
KB_START = json.dumps({"one_time": False, "buttons": [[{"action": {"type": "text", "label": "Начать анкету"}, "color": "positive"}]]})
KB_RESTART = json.dumps({"one_time": False, "buttons": [[{"action": {"type": "text", "label": "🔄 Пройти заново"}, "color": "negative"}]]})

# --- ВСПОМОГАТЕЛЬНЫЕ ---
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

# ----------------- VK СЕССИЯ С ТАЙМАУТОМ -----------------
vk_session_type = getattr(vk_api, "VkApiGroup", vk_api.VkApi)
vk_session = vk_session_type(token=VK_TOKEN)
vk = vk_session.get_api()
# Keep-alive pool aligned with the message worker count (requests defaults to 10).
vk_session.http.mount("https://", HTTPAdapter(pool_connections=30, pool_maxsize=30))

_original_request = vk_session.http.request

def _patched_request(method, url, **kwargs):
    kwargs.setdefault("timeout", 5)
    return _original_request(method, url, **kwargs)

vk_session.http.request = _patched_request

# ----------------- ПУЛ СОЕДИНЕНИЙ БД -----------------
db_pool = None
_inbox_context = threading.local()
_inbox_wake = threading.Event()
_outbox_wake = threading.Event()

class _BorrowedConnection:
    """Connection facade that keeps handler writes inside the inbox transaction."""
    def __init__(self, connection):
        self.connection = connection

    def cursor(self, *args, **kwargs):
        return self.connection.cursor(*args, **kwargs)

    def commit(self):
        # The inbox worker commits the full state + outbox transaction.
        return None

    def rollback(self):
        return self.connection.rollback()

def _current_inbox_context():
    return getattr(_inbox_context, "current", None)

def init_db_pool():
    global db_pool
    dsn = DATABASE_URL
    if "?" in dsn:
        dsn += "&connect_timeout=5"
    else:
        dsn += "?connect_timeout=5"
    db_pool = psycopg2_pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=40,
        dsn=dsn,
        options="-c statement_timeout=10000"
    )

def get_db():
    context = _current_inbox_context()
    if context is not None:
        return context["borrowed_connection"]
    return db_pool.getconn()

def release_db(conn):
    context = _current_inbox_context()
    if context is not None and conn is context["borrowed_connection"]:
        return
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
        c.execute("""CREATE TABLE IF NOT EXISTS bot_inbox (
                id BIGSERIAL PRIMARY KEY,
                peer_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                vk_message_id BIGINT NOT NULL,
                vk_date BIGINT NOT NULL,
                text TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_error TEXT,
                UNIQUE (peer_id, vk_message_id))""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_bot_inbox_pending ON bot_inbox(status, available_at, id)")
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_bot_inbox_user_order "
            "ON bot_inbox(user_id, vk_date, vk_message_id, id) WHERE status='pending'"
        )
        c.execute("""CREATE TABLE IF NOT EXISTS bot_outbox (
                id BIGSERIAL PRIMARY KEY,
                inbox_id BIGINT NOT NULL REFERENCES bot_inbox(id),
                sequence INTEGER NOT NULL,
                peer_id BIGINT NOT NULL,
                message TEXT NOT NULL,
                keyboard TEXT,
                attachment TEXT,
                random_id BIGINT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                locked_at TIMESTAMPTZ,
                sent_at TIMESTAMPTZ,
                last_error TEXT,
                UNIQUE (inbox_id, sequence))""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_bot_outbox_pending ON bot_outbox(status, available_at, id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_bot_outbox_peer_order ON bot_outbox(peer_id, id, status)")
        c.execute("UPDATE bot_outbox SET status='pending', locked_at=NULL, available_at=now(), "
                  "last_error='Retrying previously failed send' WHERE status='failed'")
        c.execute("UPDATE bot_outbox SET status='pending', locked_at=NULL, available_at=now() "
                  "WHERE status='sending' AND locked_at < now() - interval '2 minutes'")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_db(conn)

# ----------------- КЭШ ПРОГРЕССА В ПАМЯТИ -----------------
progress_cache = {}
cache_lock = threading.Lock()
progress_cache_ready = False

def _load_progress_from_db(user_id):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT step_index, uni_page, started FROM progress WHERE user_id=%s", (user_id,))
        row = c.fetchone()
        if row:
            return {
                "step_index": row[0] if row[0] is not None else 0,
                "uni_page": row[1] if row[1] is not None else 0,
                "started": row[2] if row[2] is not None else 0
            }
        return {"step_index": 0, "uni_page": 0, "started": 0}
    finally:
        release_db(conn)

def preload_progress_cache():
    global progress_cache_ready
    cache_started = time.monotonic()
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("SELECT user_id, step_index, uni_page, started FROM progress")
        rows = c.fetchall()
    except Exception:
        log.exception("Не удалось предварительно загрузить кэш прогресса")
        return
    finally:
        release_db(conn)

    with cache_lock:
        for user_id, step_index, uni_page, row_started in rows:
            progress_cache[user_id] = {
                "step_index": step_index if step_index is not None else 0,
                "uni_page": uni_page if uni_page is not None else 0,
                "started": row_started if row_started is not None else 0
            }
        progress_cache_ready = True
    log.info(
        "Кэш прогресса загружен: %s пользователей за %.3fs",
        len(rows), time.monotonic() - cache_started
    )

def get_progress_cached(user_id):
    with cache_lock:
        p = progress_cache.get(user_id)
        cache_ready = progress_cache_ready
    if p is None:
        loaded = (
            {"step_index": 0, "uni_page": 0, "started": 0}
            if cache_ready else _load_progress_from_db(user_id)
        )
        with cache_lock:
            p = progress_cache.setdefault(user_id, loaded)
    return p["step_index"], p["uni_page"], p["started"]

def _update_progress_cache(user_id, step_index, uni_page, started):
    context = _current_inbox_context()
    progress = {"step_index": step_index, "uni_page": uni_page, "started": started}
    if context is not None:
        context["progress_updates"][user_id] = progress
        return
    with cache_lock:
        progress_cache[user_id] = progress

def set_progress_cached(user_id, step_index, uni_page=0, started=1):
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
    except Exception:
        conn.rollback()
        raise
    finally:
        release_db(conn)
    _update_progress_cache(user_id, step_index, uni_page, started)

# ----------------- ОТПРАВКА СООБЩЕНИЙ -----------------
_vk_thread = threading.local()
_vk_send_rate_lock = threading.Lock()
_vk_next_send_at = 0.0
_VK_SEND_INTERVAL = 0.055  # Около 18 отправок/с, ниже лимита сообщества.

def _get_message_vk():
    api = getattr(_vk_thread, "api", None)
    if api is None:
        session_type = getattr(vk_api, "VkApiGroup", vk_api.VkApi)
        session = session_type(token=VK_TOKEN, api_version=vk_session.api_version)
        session.http.mount("https://", HTTPAdapter(pool_connections=1, pool_maxsize=1))
        original_request = session.http.request

        def request_with_timeout(method, url, **kwargs):
            kwargs.setdefault("timeout", 5)
            return original_request(method, url, **kwargs)

        session.http.request = request_with_timeout
        _vk_thread.session = session
        _vk_thread.api = session.get_api()
        api = _vk_thread.api
    return api

def _wait_for_vk_send_slot():
    global _vk_next_send_at
    with _vk_send_rate_lock:
        now = time.monotonic()
        slot = max(now, _vk_next_send_at)
        _vk_next_send_at = slot + _VK_SEND_INTERVAL
    if slot > now:
        time.sleep(slot - now)

def send_message(user_id, message, keyboard=None, attachment=None, random_id=None):
    context = _current_inbox_context()
    if context is not None:
        context["replies"].append({
            "peer_id": user_id,
            "message": message,
            "keyboard": keyboard,
            "attachment": attachment
        })
        return True

    started = time.monotonic()
    try:
        _wait_for_vk_send_slot()
        params = {
            "peer_id": user_id,
            "message": message,
            "random_id": random_id if random_id is not None else get_random_id()
        }
        if keyboard:
            params["keyboard"] = keyboard
        if attachment:
            params["attachment"] = attachment
        _get_message_vk().messages.send(**params)
        elapsed = time.monotonic() - started
        if elapsed >= 1:
            log.warning("Медленная отправка VK peer=%s duration=%.3fs", user_id, elapsed)
        return True
    except Exception as exc:
        elapsed = time.monotonic() - started
        error_code = getattr(exc, "code", None)
        if error_code is None:
            error_data = getattr(exc, "error", None)
            if isinstance(error_data, dict):
                error_code = error_data.get("error_code")
        if str(error_code) == "901":
            log.error(
                "VK запретил отправку peer=%s: пользователь не разрешил сообщения "
                "от сообщества (ApiError 901)", user_id
            )
            return None
        log.exception("Ошибка отправки VK peer=%s duration=%.3fs", user_id, elapsed)
        return False

# ----------------- ВУЗы -----------------
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
    "Камбарский Машиностроительный Колледж (филиал)",
    "БПОУ УР «Радиомеханический техникум имени В.А. Шутова»",
    "АПОУ УР «Экономико-технологический колледж»",
    "БПОУ УР «Асановский аграрно-технический техникум»",
    "АПОУ УР «Топливно-энергетический колледж»",
    "БПОУ УР «Ижевский техникум индустрии питания»",
    "КПОУ УР «Удмуртский республиканский колледж культуры»",
    "АНПОО «Международный Восточно-Европейский колледж»",
    "АПОУ УР «Техникум радиоэлектроники и информационных технологий им. А.В. Воскресенского»",
    "Можгинский филиал АПОУ УР Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной",
    "Сарапульский филиал АПОУ УР Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной",
    "АПОУ УР «Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной Министерства здравоохранения Удмуртской Республики»",
    "Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной, Глазовский филиал",
    "Воткинский филиал АПОУ УР «Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной Министерства здравоохранения Удмуртской Республики»",
    "ПОЧУ «Ижевский техникум экономики, управления и права Удмуртпотребсоюза»",
    "АНПОО СПО «Ижевский финансово-юридический колледж»",
    "БПОУ УР «Удмуртский республиканский социально-педагогический колледж»",
    "АПОУ УР «Строительный техникум»",
    "ФГБОУ ВО «Ижевская государственная медицинская академия»",
    "КПОУ УР «Республиканский музыкальный колледж»",
    "БПОУ УР «Ижевский промышленно-экономический колледж» в г. Можга",
    "БПОУ УР «Можгинский педагогический колледж имени Т.К. Борисова»",
    "БПОУ УР «Можгинский агропромышленный колледж»",
    "БПОУ УР «Сарапульский политехнический техникум»",
    "БПОУ УР «Сарапульский многопрофильный колледж»",
    "БПОУ УР «Сарапульский колледж социально-педагогических технологий и сервиса»",
    "БПОУ УР «Сюмсинский техникум лесного и сельского хозяйства»",
    "БПОУ УР «Увинский профессиональный колледж»",
    "БПОУ УР «Ярский политехникум»",
    "ФГБОУ ВО «Приволжский государственный университет путей сообщения»",
    "БПОУ УР «Ижевский индустриальный техникум имени Евгения Фёдоровича Драгунова»",
    "БПОУ УР «Глазовский политехнический колледж»",
    "Ижевский институт (филиал) ВГУЮ (РПА Минюста России)",
    "ФГБОУ ВО «Удмуртский государственный университет»",
    "ФГБОУ ВО «Удмуртский государственный университет» филиал в г.Воткинске",
    "ФГБОУ ВО «Удмуртский государственный аграрный университет»",
    "ФГБОУ ВО «Ижевский государственный технический университет имени М.Т. Калашникова»",
    "Камбарский филиал ФГБОУ ВО «Ижевский государственный технический университет имени М.Т. Калашникова»",
    "Глазовский инженерно-экономический институт(филиал) ФГБОУ ВО «ИжГТУ имени М.Т. Калашникова»",
    "БПОУ УР «Ижевский автотранспортный техникум»",
    "ФГБОУ ВО «Глазовский государственный инженерно-педагогический университет имени В.Г. Короленко»",
    "Сарапульский техникум машиностроения и информационных технологий"
]
ITEMS_PER_PAGE = 10

# ----------------- РАЙОНЫ УДМУРТИИ -----------------
DISTRICTS_UNIVERSITIES = {
    "Ижевск": [
        "БПОУ УР «Ижевский торгово-экономический техникум»",
        "БПОУ УР «Ижевский монтажный техникум»",
        "БПОУ «Ижевский агростроительный техникум»",
        "ЧПОО «Нефтяной техникум»",
        "БПОУ УР «Ижевский политехнический колледж»",
        "БПОУ УР «Ижевский промышленно-экономический колледж»",
        "БПОУ УР «Ижевский машиностроительный техникум им. С.Н. Борина»",
        "БПОУ УР «Радиомеханический техникум имени В.А. Шутова»",
        "АПОУ УР «Экономико-технологический колледж»",
        "АПОУ УР «Топливно-энергетический колледж»",
        "БПОУ УР «Ижевский техникум индустрии питания»",
        "КПОУ УР «Удмуртский республиканский колледж культуры»",
        "АНПОО «Международный Восточно-Европейский колледж»",
        "АПОУ УР «Техникум радиоэлектроники и информационных технологий им. А.В. Воскресенского»",
        "АПОУ УР «Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной Министерства здравоохранения Удмуртской Республики»",
        "ПОЧУ «Ижевский техникум экономики, управления и права Удмуртпотребсоюза»",
        "АНПОО СПО «Ижевский финансово-юридический колледж»",
        "БПОУ УР «Удмуртский республиканский социально-педагогический колледж»",
        "АПОУ УР «Строительный техникум»",
        "Министерство юстиции Российской Федерации",
        "ФГБОУ ВО «Ижевская государственная медицинская академия»",
        "КПОУ УР «Республиканский музыкальный колледж»",
        "ФГБОУ ВО «Приволжский государственный университет путей сообщения»",
        "БПОУ УР «Ижевский индустриальный техникум имени Евгения Фёдоровича Драгунова»",
        "Ижевский институт (филиал) ВГУЮ (РПА Минюста России)",
        "ФГБОУ ВО «Удмуртский государственный университет»",
        "БПОУ УР «Ижевский автотранспортный техникум»",
        "ФГБОУ ВО «Удмуртский государственный аграрный университет»",
        "ФГБОУ ВО «Ижевский государственный технический университет имени М.Т. Калашникова»",
    ],
    "Воткинск": [
        "БПОУ УР «Воткинский промышленный техникум»",
        "БПОУ УР «Воткинский музыкально-педагогический колледж имени П.И. Чайковского»",
        "ФГБОУ ВО «Удмуртский государственный университет» филиал в г.Воткинске",
        "Воткинский филиал АПОУ УР «Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной Министерства здравоохранения Удмуртской Республики»",
        "БПОУ УР «Воткинский машиностроительный техникум имени В.Г.Садовникова»",
    ],
    "Глазов": [
        "АПОУ УР «Глазовский аграрно-промышленный техникум»",
        "БПОУ УР «Глазовский технический колледж»",
        "БПОУ УР «Глазовский политехнический колледж»",
        "ФГБОУ ВО «Глазовский государственный инженерно-педагогический университет имени В. Г. Корененко»",
        "Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной, Глазовский филиал",
        "ФГБОУ ВО «Глазовский государственный инженерно-педагогический университет имени В. Г. Короленко»",
        "ФГБОУ ВО «Глазовский государственный инженерно-педагогический университет имени В.Г. Короленко»",
        "Глазовский инженерно-экономический институт(филиал) ФГБОУ ВО «ИжГТУ имени М.Т. Калашникова»",
    ],
    "Можга": [
        "БПОУ УР «Ижевский промышленно-экономический колледж» в г. Можга",
        "БПОУ УР «Можгинский педагогический колледж имени Т.К. Борисова»",
        "Филиал АПОУ УР Ижевский промышленно-экономический колледж в г.Можга",
        "Можгинский филиал АПОУ УР Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной",
        "БПОУ УР «Можгинский агропромышленный колледж»",
    ],
    "Сарапул": [
        "БПОУ УР «Сарапульский политехнический техникум»",
        "БПОУ УР «Сарапульский многопрофильный колледж»",
        "Сарапульский филиал АПОУ УР Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной",
        "БПОУ УР «Сарапульский колледж социально-педагогических технологий и сервиса»",
        "Сарапульский техникум машиностроения и информационных технологий",
    ],
    "Алнаши": ["БПОУ УР «Асановский аграрно-технический техникум»"],
    "Дебесы": ["БПОУ «Дебесский политехникум»"],
    "Кез": ["Филиал БПОУ УР «Дебесский политехникум» п. Кез"],
    "Камбарка": [
        "ФГБОУ ВО «Ижевский государственный технический университет имени М.Т. Калашникова» Камбарский филиал",
        "Камбарский Машиностроительный Колледж (филиал)"
        "Камбарский филиал ФГБОУ ВО «Ижевский государственный технический университет имени М.Т. Калашникова»",
        "Камбарский филиал ФГБОУ ВО «Ижевский государственный технический университет имени М.Т. Калашникова»Глазовский инженерно-экономический институт(филиал) ФГБОУ ВО «ИжГТУ имени М.Т. Калашникова»",
    ],
    "Игра": ["БПОУР «Игринский политехнический техникум»"],
    "Сюмси": ["БПОУ УР «Сюмсинский техникум лесного и сельского хозяйства»"],
    "Ува": ["БПОУ УР «Увинский профессиональный колледж»"],
    "Яр": ["БПОУ УР «Ярский политехникум»"],
}

INSTITUTION_TO_DISTRICT = {}
for _district, _unis in DISTRICTS_UNIVERSITIES.items():
    for _uni in _unis:
        INSTITUTION_TO_DISTRICT[_uni] = _district

# ----------------- ШАГИ АНКЕТЫ -----------------
STEPS = [
    "fio", "consent", "institution", "specialty", "study_group",
    "course", "form_of_study", "contacts",
    "employment_status", "target_contract", "experience",
    "practice_eval", "events", "resume_status",
    "interview_training", "special_status", "military",
    "maternity", "graduate", "post_plans", "help_needed"
]

STEP_TO_DB = {s: s for s in STEPS}

QUESTIONS = {
    "fio": "Пожалуйста, укажите ваши фамилию, имя и отчество полностью:",
    "consent": (
        "Я, {fio}, на основании статей 9, 11 Федерального закона от 27 июля 2006 г. N 152-ФЗ "
        "\"О персональных данных\" в целях моей профессиональной ориентации даю свое согласие "
        "казенному учреждению Удмуртской Республики «Республиканский центр занятости населения» "
        "на автоматизированную, а также без использования средств автоматизации обработку своих "
        "персональных данных, включая сбор, систематизацию, накопление, хранение, уточнение "
        "(обновление, изменение), использование, обезличивание, блокирование, уничтожение "
        "персональных данных о моих фамилии, имени, отчестве, номере телефона, адресе электронной почты.\n\n"
        "Настоящее согласие действует в течение 1 года с даты анкетирования.\n\n"
        "Пожалуйста, подтвердите согласие, нажав кнопку ниже:"
    ),
    "institution": "Выберите ваше учебное заведение из списка (используйте «далее» / «назад» для пролистывания):",
    "specialty": "Укажите вашу специальность обучения:",
    "study_group": "Укажите номер вашей учебной группы:",
    "course": "Выберите ваш курс обучения:",
    "form_of_study": "Выберите форму обучения:",
    "contacts": "Укажите контактные данные — телефон и/или e-mail (например: +79991234567 student@mail.ru). Можно указать оба контакта через пробел или запятую:",
    "employment_status": "Ваш статус занятости прямо сейчас. Выберите один вариант, указав его номер:",
    "target_contract": "Есть ли у вас заключённый договор о целевом обучении с работодателем?",
    "experience": "Есть ли у вас опыт работы или оплачиваемой стажировки по основной или близкой к ней специальности?",
    "practice_eval": "Как вы в целом оцениваете результаты своих производственных практик у работодателей по специальности обучения?",
    "events": "Участвовали ли вы в течение обучения в мероприятиях, которые помогают познакомиться с работодателями (ярмарки вакансий, дни карьеры, профтуры на предприятия, встречи с работодателями и т.д.)?",
    "resume_status": "Наличие резюме для поиска работы:",
    "interview_training": "Проходили ли вы занятия или тренинги по навыкам прохождения собеседования?",
    "special_status": "Есть ли у вас особый статус или жизненные обстоятельства?",
    "military": "Планируется ли в отношении вас призыв на военную службу в ближайшее время (после окончания текущего года обучения)?",
    "maternity": "Планируете ли вы уходить в отпуск по уходу за ребёнком в период обучения или сразу после окончания обучения (или продолжать уже начатый отпуск)?",
    "graduate": "Являетесь ли вы студентом выпускного курса (оканчиваете программу в текущем учебном году)?",
    "post_plans": "Ваши планы после выпуска. Выберите один или несколько вариантов, указав их номера через запятую (например: 1, 3):",
    "help_needed": "Какую помощь от Кадрового центра «Работа России» вы бы считали наиболее полезной? Выберите все подходящие варианты, указав их номера через запятую (например: 1, 2, 4):"
}

OPTIONS = {
    "consent": ["Да, я согласен(на)", "Нет, я не согласен(на)"],
    "course": ["1 курс", "2 курс", "3 курс", "4 курс", "5 курс"],
    "form_of_study": ["очная", "очно-заочная", "заочная"],
    "employment_status": [
        "Работаю по трудовому договору (в том числе по совместительству)",
        "Работаю по гражданско-правовому договору (договор подряда, услуг и т.п.)",
        "Являюсь самозанятым / ИП / учредителем юрлица",
        "Прохожу оплачиваемую стажировку / практику у работодателя",
        "Работаю временно (разовые подработки), не по специальности обучения",
        "Ничего из вышеперечисленного"
    ],
    "target_contract": ["да, договор о целевом обучении заключён", "нет, договора о целевом обучении нет"],
    "experience": [
        "да, есть опыт работы / оплачиваемой стажировки по основной или близкой специальности",
        "есть опыт работы только вне специальности обучения",
        "нет, опыта работы и оплачиваемых стажировок пока не было"
    ],
    "practice_eval": [
        "скорее доволен(льна) или полностью доволен(льна)",
        "скорее не доволен(на) / совсем не доволен(льна) результатами практик",
        "не проходил(а) производственную практику"
    ],
    "events": [
        "да, за последний год участвовал(а) хотя бы в одном таком мероприятии",
        "участвовал(а), но более года назад",
        "нет, ещё ни разу не участвовал(а)"
    ],
    "resume_status": [
        "есть актуальное резюме, которым я пользуюсь или готов(а) пользоваться",
        "резюме есть, но оно устарело / резюме нет, я его не составлял(а)"
    ],
    "interview_training": ["да, проходил(а) одно или несколько таких мероприятий", "пока не проходил(а)"],
    "special_status": [
        "да, имею группу инвалидности",
        "отношусь к категории детей-сирот и детей, оставшихся без попечения родителей",
        "планирую переезд в другой регион / страну после окончания обучения",
        "ничего из вышеперечисленного"
    ],
    "military": ["да, планируется призыв", "нет / не подлежу призыву / вопрос уже решён (служба пройдена и др.)"],
    "maternity": ["да, планирую", "пока не планирую"],
    "graduate": ["да, я учусь на выпускном курсе", "нет, я не на выпускном курсе"],
    "post_plans": [
        "У меня есть подписанный трудовой договор (или договор на целевое обучение)",
        "Есть устная договорённость с работодателем, но без подписанных документов",
        "Прохожу стажировку",
        "Планирую организовать своё дело (самозанятость / ИП / учредитель юрлица)",
        "Планирую продолжить обучение (магистратура / аспирантура и пр.)",
        "Сейчас ищу работу",
        "Пока нет планов"
    ],
    "help_needed": [
        "Подбор актуальных вакансий с учётом специальности",
        "Тренинги по составлению резюме, подготовке к собеседованиям, сопроводительных писем",
        "Профтур-экскурсии на предприятия",
        "Подбор оплачиваемой стажировки",
        "Подбор работодателя для практики",
        "Заключение договора с работодателем на целевое обучение",
        "Помощь с ЕЦП «Работа России»",
        "Другое (укажите)"
    ]
}

MULTI_STEPS = ["post_plans", "help_needed"]

MESSAGES = {
    "welcome": (
        "👋 Здравствуйте!\n\n"
        "Я задам несколько коротких вопросов о вашем обучении и занятости. "
        "Это займёт примерно 5–7 минут. По итогу анкетирования кадровый центр "
        "«Работа России» поможет Вам в прохождении тестирования на определение "
        "склонностей к профессиям, в составлении грамотного резюме, "
        "а также в подборе подходящих вакансий.\n\n"
        "Нажмите кнопку «Начать анкету», чтобы приступить."
    ),
    "invalid_contact": (
        "Не удалось распознать контакты. Пожалуйста, введите:\n\n"
        "• Номер телефона в формате +79991234567 или 89991234567\n"
        "и/или\n"
        "• Адрес электронной почты в формате example@mail.ru\n\n"
        "Можно указать оба контакта через пробел или запятую."
    ),
    "invalid_number": "Пожалуйста, введите номер от 1 до {}.",
    "invalid_multi": "Пожалуйста, укажите номера вариантов через запятую (например: 1, 3, 5). Проверьте, что номера от 1 до {}.",
    "no_data": "Пока нет собранных анкет для выгрузки.",
    "no_data_today": "Сегодня пока нет новых анкет для выгрузки.",
    "admin_only": "Эта команда доступна только администраторам.",
    "finished": "✅ Спасибо! Анкета заполнена.\n\nПредоставленная вами информация позволит нам детально проанализировать ситуацию и предложить оптимальное решение.\n\nЕсли хотите пройти анкету заново — нажмите кнопку ниже.",
    "already_finished": "✅ Вы уже заполнили анкету ранее. Если хотите пройти заново — нажмите кнопку ниже.",
}

# ----------------- ОТПРАВКА И ВОПРОСЫ -----------------

def ask_university_page(user_id, page):
    page = page if isinstance(page, int) else 0
    start = page * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, len(UNIVERSITIES))
    items = UNIVERSITIES[start:end]
    list_text = format_numbered_list(items, start_from=start + 1, truncate=True)
    nav = []
    if page > 0:
        nav.append("«назад» — предыдущая страница")
    if end < len(UNIVERSITIES):
        nav.append("«далее» — следующая страница")
    nav_text = "\n".join(nav) if nav else ""
    message = f"{QUESTIONS['institution']}\n\n{list_text}"
    if nav_text:
        message += f"\n\n{nav_text}"
    message += "\n\nВведите номер вашего учебного заведения."
    send_message(user_id, message)

def ask_step(user_id, step_key, uni_page=0, fio_text=None):
    uni_page = uni_page if isinstance(uni_page, int) else 0
    if step_key == "institution":
        ask_university_page(user_id, uni_page)
    elif step_key == "consent":
        if fio_text is None:
            conn = get_db()
            try:
                c = conn.cursor()
                c.execute("SELECT fio FROM answers WHERE user_id=%s", (user_id,))
                row = c.fetchone()
                fio_text = row[0] if row and row[0] else "[ФИО не указано]"
            finally:
                release_db(conn)
        message = QUESTIONS["consent"].format(fio=fio_text)
        opts = OPTIONS["consent"]
        list_text = format_numbered_list(opts, truncate=False)
        hint = "Напишите номер выбранного варианта (1 или 2)."
        send_message(user_id, f"{message}\n\n{list_text}\n\n{hint}")
    elif step_key in OPTIONS:
        opts = OPTIONS[step_key]
        list_text = format_numbered_list(opts, truncate=False)
        if step_key in MULTI_STEPS:
            hint = "Напишите номера выбранных вариантов через запятую (например: 1, 3)."
        else:
            hint = "Напишите номер выбранного варианта (например: 1)."
        message = f"{QUESTIONS[step_key]}\n\n{list_text}\n\n{hint}"
        send_message(user_id, message)
    else:
        send_message(user_id, QUESTIONS[step_key])

# ----------------- СОХРАНЕНИЕ ОТВЕТА + СДВИГ ПРОГРЕССА В ОДНОЙ ТРАНЗАКЦИИ -----------------
def save_and_advance(user_id, field, value, step_index, uni_page=0, fio_text=None):
    db_started = time.monotonic()
    conn = get_db()
    try:
        c = conn.cursor()
        # Сохраняем ответ
        if field == "consent_status":
            c.execute(
                "INSERT INTO answers (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING",
                (user_id,)
            )
            c.execute(
                "UPDATE answers SET consent_status=%s WHERE user_id=%s",
                (value, user_id)
            )
        elif field:
            c.execute(
                f"INSERT INTO answers (user_id, {field}) VALUES (%s, %s) "
                f"ON CONFLICT (user_id) DO UPDATE SET {field}=EXCLUDED.{field}",
                (user_id, value)
            )
        # Сдвигаем прогресс
        next_idx = step_index + 1
        if next_idx >= len(STEPS):
            # Анкета завершена
            c.execute("UPDATE answers SET created_at=%s WHERE user_id=%s", (datetime.now(), user_id))
            c.execute(
                "INSERT INTO progress (user_id, step_index, uni_page, started, started_at) "
                "VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT (user_id) DO UPDATE SET step_index=%s, uni_page=%s, started=%s, "
                "started_at=COALESCE(progress.started_at, EXCLUDED.started_at)",
                (user_id, next_idx, 0, 2, datetime.now(), next_idx, 0, 2)
            )
            conn.commit()
        else:
            c.execute(
                "INSERT INTO progress (user_id, step_index, uni_page, started, started_at) "
                "VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT (user_id) DO UPDATE SET step_index=%s, uni_page=%s, started=%s, "
                "started_at=COALESCE(progress.started_at, EXCLUDED.started_at)",
                (user_id, next_idx, 0, 1, datetime.now(), next_idx, 0, 1)
            )
            conn.commit()
        _update_progress_cache(
            user_id, next_idx, 0, 2 if next_idx >= len(STEPS) else 1
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        release_db(conn)
        log.info(
            "db_write user=%s duration=%.3fs",
            user_id, time.monotonic() - db_started
        )
    # Отправляем следующий шаг или сообщение о завершении
    if next_idx >= len(STEPS):
        send_message(user_id, MESSAGES["finished"], KB_RESTART)
    else:
        ask_step(user_id, STEPS[next_idx], 0, fio_text)

# ----------------- ПАРСИНГ -----------------

def parse_single_number(text, max_val):
    text = text.strip()
    if text.isdigit():
        n = int(text)
        if 1 <= n <= max_val:
            return n
    return None

def parse_multi_numbers(text, max_val):
    try:
        parts = [p.strip() for p in text.split(",") if p.strip()]
        nums = [int(p) for p in parts]
        if any(n < 1 or n > max_val for n in nums):
            return None
        return nums
    except ValueError:
        return None

# ----------------- ПОДСЧЁТ БАЛЛОВ -----------------

def calculate_scores(row):
    scores = {}
    emp = (row.get("employment_status") or "").lower()
    if "трудовому договору" in emp:
        scores["employment"] = 0
    elif any(k in emp for k in ["гражданско-правовому", "самозанят", "стажировк", "временно"]):
        scores["employment"] = 0
    elif "ничего из вышеперечисленного" in emp:
        scores["employment"] = 1
    tc = (row.get("target_contract") or "").lower()
    if "да" in tc and "нет" not in tc:
        scores["target_contract"] = 0
    elif "нет" in tc:
        scores["target_contract"] = 2
    exp = (row.get("experience") or "").lower()
    if "да, есть опыт" in exp:
        scores["experience"] = 0
    elif "вне специальности" in exp:
        scores["experience"] = 3
    elif "нет, опыта" in exp:
        scores["experience"] = 3
    pe = (row.get("practice_eval") or "").lower()
    if "не доволен" in pe or "недоволен" in pe:
        scores["practice_eval"] = 2
    elif "доволен" in pe:
        scores["practice_eval"] = 0
    elif "не проходил" in pe:
        scores["practice_eval"] = 2
    ev = (row.get("events") or "").lower()
    if "за последний год" in ev:
        scores["events"] = 0
    elif "более года назад" in ev or "ни разу" in ev:
        scores["events"] = 2
    rs = (row.get("resume_status") or "").lower()
    if "актуальное" in rs:
        scores["resume"] = 0
    elif "устарело" in rs or "не составлял" in rs:
        scores["resume"] = 1
    it = (row.get("interview_training") or "").lower()
    if "да" in it and "не проходил" not in it:
        scores["interview"] = 1
    elif "не проходил" in it:
        scores["interview"] = 0
    ss = (row.get("special_status") or "").lower()
    if "ничего из вышеперечисленного" in ss:
        scores["special_status"] = 0
    elif ss:
        scores["special_status"] = 1
    mil = (row.get("military") or "").lower()
    if "да, планируется" in mil:
        scores["military"] = 1
    elif "нет" in mil or "не подлежу" in mil:
        scores["military"] = 0
    total = sum(v for v in scores.values())
    return scores, total

# ----------------- ВЫГРУЗКА -----------------

EXPORT_HEADERS = {
    "user_id": "ID пользователя", "fio": "ФИО", "institution": "Учебное заведение",
    "specialty": "Специальность", "study_group": "Учебная группа", "course": "Курс",
    "form_of_study": "Форма обучения", "contacts": "Контакты",
    "employment_status": "Статус занятости", "target_contract": "Целевой договор",
    "experience": "Опыт работы", "practice_eval": "Оценка практик",
    "events": "Участие в мероприятиях", "resume_status": "Наличие резюме",
    "interview_training": "Тренинги по собеседованию", "special_status": "Особый статус",
    "military": "Призыв на военную службу", "maternity": "Отпуск по уходу за ребёнком",
    "graduate": "Выпускной курс", "post_plans": "Планы после выпуска",
    "help_needed": "Нужная помощь", "created_at": "Дата заполнения",
}

def _queue_export_reply(admin_id, message, inbox_id=None, attachment=None):
    if inbox_id is None:
        return send_message(admin_id, message, attachment=attachment)

    conn = db_pool.getconn()
    try:
        c = conn.cursor()
        c.execute(
            "INSERT INTO bot_outbox "
            "(inbox_id, sequence, peer_id, message, attachment, random_id) "
            "VALUES (%s,1,%s,%s,%s,%s) ON CONFLICT (inbox_id, sequence) DO NOTHING",
            (inbox_id, admin_id, message, attachment, get_random_id())
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)
    _outbox_wake.set()
    return True


def export_to_table(admin_id, today_only=False, inbox_id=None):
    conn = get_db()
    try:
        c = conn.cursor(cursor_factory=RealDictCursor)
        if today_only:
            today = date.today()
            c.execute("SELECT a.* FROM answers a LEFT JOIN progress p ON a.user_id = p.user_id WHERE a.created_at::date = %s ORDER BY p.started_at ASC NULLS FIRST", (today,))
        else:
            c.execute("SELECT a.* FROM answers a LEFT JOIN progress p ON a.user_id = p.user_id ORDER BY p.started_at ASC NULLS FIRST")
        rows = c.fetchall()
    finally:
        release_db(conn)

    if not rows:
        _queue_export_reply(
            admin_id,
            MESSAGES["no_data_today"] if today_only else MESSAGES["no_data"],
            inbox_id
        )
        return True

    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws1 = wb.active
    ws1.title = "Анкеты"
    cols = list(rows[0].keys())
    header_font = Font(bold=True)
    for col_idx, col_name in enumerate(cols, start=1):
        cell = ws1.cell(row=1, column=col_idx, value=EXPORT_HEADERS.get(col_name, col_name))
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")
    for row_idx, r in enumerate(rows, start=2):
        for col_idx, col_name in enumerate(cols, start=1):
            val = r[col_name]
            if col_name == "created_at" and val is not None:
                val = val.strftime("%Y-%m-%d %H:%M")
            ws1.cell(row=row_idx, column=col_idx, value=val if val is not None else "")

    # Summary table in Y:AE, alongside the detailed survey export.
    def education_type(row):
        institution = str(row.get("institution") or "").strip().casefold()
        if not institution:
            return "Не указано"
        if any(marker in institution for marker in ("фгбоу во", "вгу", "университет", "академия", "институт")):
            return "ВО"
        return "СПО"

    grouped_rows = {"СПО": [], "ВО": [], "Не указано": []}
    for row in rows:
        grouped_rows[education_type(row)].append(row)

    ws1["Y1"] = "Студенты"
    ws1["Z1"] = (
        "Количество студентов ПОО, в отношении которых проведена "
        "оценка риска нетрудоустройства, чел."
    )
    ws1["AB1"] = "Число студентов ПОО, находящихся под риском нетрудоустройства, чел."
    ws1["Z2"] = "Всего"
    ws1["AA2"] = "Из них"
    ws1["AA4"] = "Студентов выпускных курсов"
    ws1["AB2"] = "Всего"
    ws1["AC2"] = "Из них"
    ws1["AC3"] = (
        "Студентов, призывающихся на военную службу или собирающихся "
        "осуществлять уход за ребенком"
    )
    ws1["AD3"] = "Студентов выпускных курсов"
    ws1["AE3"] = "Из них"
    ws1["AE4"] = (
        "Студентов, призывающихся на военную службу или собирающихся "
        "осуществлять уход за ребенком"
    )
    for merged_range in (
        "Y1:Y4", "Z1:AA1", "Z2:Z4", "AA2:AA3", "AB1:AE1", "AB2:AB4",
        "AC2:AE2", "AC3:AC4", "AD3:AD4"
    ):
        ws1.merge_cells(merged_range)

    thin_border = Border(
        left=Side(style="thin", color="000000"),
        right=Side(style="thin", color="000000"),
        top=Side(style="thin", color="000000"),
        bottom=Side(style="thin", color="000000")
    )
    summary_header_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    for row in ws1.iter_rows(min_row=1, max_row=4, min_col=25, max_col=31):
        for cell in row:
            cell.font = Font(bold=True, size=10)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.fill = summary_header_fill
            cell.border = thin_border

    summary_levels = ["СПО", "ВО"]
    if grouped_rows["Не указано"]:
        summary_levels.append("Не указано")
    for row_idx, level in enumerate(summary_levels, start=5):
        level_rows = grouped_rows[level]
        risk_rows = [r for r in level_rows if calculate_scores(dict(r))[1] >= 5]
        graduate_rows = [r for r in level_rows if str(r.get("graduate") or "").strip().casefold().startswith("да")]
        risk_graduates = [r for r in risk_rows if str(r.get("graduate") or "").strip().casefold().startswith("да")]

        def has_special_circumstances(row):
            return any(
                str(row.get(field) or "").strip().casefold().startswith("да")
                for field in ("military", "maternity")
            )

        values = (
            level,
            len(level_rows),
            len(graduate_rows),
            len(risk_rows),
            sum(1 for r in risk_rows if has_special_circumstances(r)),
            len(risk_graduates),
            sum(1 for r in risk_graduates if has_special_circumstances(r))
        )
        for col_idx, value in enumerate(values, start=25):
            cell = ws1.cell(row=row_idx, column=col_idx, value=value)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = thin_border

    ws1.row_dimensions[1].height = 60
    ws1.row_dimensions[2].height = 32
    ws1.row_dimensions[3].height = 75
    ws1.row_dimensions[4].height = 75
    for col_idx in range(1, ws1.max_column + 1):
        ws1.column_dimensions[get_column_letter(col_idx)].width = 25
    for column, width in {
        "Y": 14, "Z": 12, "AA": 15, "AB": 12, "AC": 27, "AD": 16, "AE": 30
    }.items():
        ws1.column_dimensions[column].width = width

    bold_font = Font(bold=True)
    total_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    district_headers = ["ФИО","Учебное заведение","Контакты","Статус занятости","Целевой договор","Опыт работы","Оценка практик","Мероприятия","Резюме","Собеседование","Особый статус","Военный призыв","Сумма баллов"]
    score_keys = ["employment","target_contract","experience","practice_eval","events","resume","interview","special_status","military"]

    def write_score_sheet(workbook, sheet_name, sheet_rows):
        ws = workbook.create_sheet(title=sheet_name)
        for col_idx, h in enumerate(district_headers, start=1):
            cell = ws.cell(row=1, column=col_idx, value=h)
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center")
        sorted_rows = sorted(sheet_rows, key=lambda r: r.get("created_at") or datetime.min)
        row_idx = 2
        for r in sorted_rows:
            scores, total = calculate_scores(dict(r))
            ws.cell(row=row_idx, column=1, value=r.get("fio") or "")
            ws.cell(row=row_idx, column=2, value=r.get("institution") or "")
            ws.cell(row=row_idx, column=3, value=r.get("contacts") or "")
            for i, key in enumerate(score_keys, start=4):
                val = scores.get(key)
                ws.cell(row=row_idx, column=i, value=val if val is not None else "")
            total_cell = ws.cell(row=row_idx, column=13, value=total)
            total_cell.font = bold_font
            total_cell.fill = total_fill
            row_idx += 1
        for col_idx in range(1, len(district_headers) + 1):
            ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = 25

    for district_name in DISTRICTS_UNIVERSITIES:
        district_rows = [r for r in rows if INSTITUTION_TO_DISTRICT.get(r.get("institution")) == district_name]
        if district_rows:
            write_score_sheet(wb, district_name, district_rows)
    other_rows = [r for r in rows if r.get("institution") and INSTITUTION_TO_DISTRICT.get(r.get("institution")) is None]
    if other_rows:
        write_score_sheet(wb, "Прочие", other_rows)

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_file:
        fname = temp_file.name
    try:
        wb.save(fname)
        if not os.path.exists(fname) or os.path.getsize(fname) == 0:
            raise RuntimeError("Не удалось создать файл выгрузки.")

        api = _get_message_vk()
        result = None
        max_upload_attempts = 3
        for attempt in range(1, max_upload_attempts + 1):
            upload_server = api.docs.getMessagesUploadServer(type='doc', peer_id=admin_id)
            upload_url = upload_server['upload_url']
            with open(fname, "rb") as f:
                resp = requests.post(
                    upload_url,
                    files={"file": ("survey_export.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                    timeout=130
                )
            resp.raise_for_status()
            result = resp.json()
            if result.get("file"):
                break

            upload_error = str(result).casefold()
            if "no_free_space" not in upload_error or attempt == max_upload_attempts:
                raise RuntimeError(f"VK не принял файл: {result}")
            delay = 2 ** (attempt - 1)
            log.warning(
                "VK upload server has no free space; retry %s/%s in %ss",
                attempt, max_upload_attempts, delay
            )
            time.sleep(delay)

        file_title = f"Выгрузка за {date.today().strftime('%d.%m.%Y')}" if today_only else "Выгрузка анкет"
        saved = api.docs.save(file=result["file"], title=file_title)
        if isinstance(saved, dict) and "doc" in saved:
            document = saved["doc"]
        elif isinstance(saved, dict) and saved.get("docs"):
            document = saved["docs"][0]
        else:
            raise RuntimeError(f"Неожиданный ответ docs.save: {saved}")
        attachment = f"doc{document['owner_id']}_{document['id']}"

        sheet_list = []
        for district_name in DISTRICTS_UNIVERSITIES:
            count = sum(1 for r in rows if INSTITUTION_TO_DISTRICT.get(r.get("institution")) == district_name)
            if count:
                sheet_list.append(f"  • «{district_name}» — {count} чел.")
        if other_rows:
            sheet_list.append(f"  • «Прочие» — {len(other_rows)} чел.")
        sheets_text = "\n".join(sheet_list) if sheet_list else ""
        header = f"📊 Выгрузка анкет за сегодня ({date.today().strftime('%d.%m.%Y')}):\n\n" if today_only else "📊 Вот полная выгрузка анкет:\n\n"
        _queue_export_reply(
            admin_id,
            f"{header}• Лист «Анкеты» — полные ответы\n• Листы по районам — ФИО, баллы:\n\n{sheets_text}\n\nВсего анкет: {len(rows)}",
            inbox_id,
            attachment
        )
        log.info("Файл выгрузки загружен и поставлен в outbox admin=%s rows=%s", admin_id, len(rows))
        return True
    finally:
        try:
            os.remove(fname)
        except OSError:
            log.warning("Не удалось удалить временный файл выгрузки: %s", fname)

def _run_export_task(admin_id, today_only, inbox_id=None):
    log.info("Запуск задачи выгрузки admin=%s today_only=%s", admin_id, today_only)
    try:
        export_to_table(admin_id, today_only, inbox_id)
        log.info("Задача выгрузки завершена admin=%s today_only=%s", admin_id, today_only)
    except Exception as exc:
        log.exception("Не удалось подготовить выгрузку для admin=%s", admin_id)
        if "no_free_space" in str(exc).casefold():
            failure_message = (
                "VK сформировал отказ из-за отсутствия свободного места на сервере загрузки. "
                "Файл не удалось передать; попробуйте повторить выгрузку позже."
            )
        else:
            failure_message = "❌ Не удалось сформировать или отправить выгрузку. Подробности записаны в журнал."
        try:
            _queue_export_reply(
                admin_id,
                failure_message,
                inbox_id
            )
        except Exception:
            log.exception("Не удалось поставить сообщение об ошибке выгрузки в outbox admin=%s", admin_id)


def _submit_export(admin_id, today_only, inbox_id=None):
    try:
        if _executor_heavy is None:
            raise RuntimeError("Фоновый исполнитель выгрузки ещё не запущен")
        _executor_heavy.submit(_run_export_task, admin_id, today_only, inbox_id)
        log.info("Задача выгрузки поставлена в очередь admin=%s today_only=%s", admin_id, today_only)
    except Exception:
        log.exception("Не удалось запустить выгрузку для admin=%s", admin_id)
        try:
            _queue_export_reply(
                admin_id,
                "❌ Не удалось запустить выгрузку. Попробуйте позже.",
                inbox_id
            )
        except Exception:
            log.exception("Не удалось поставить сообщение о запуске выгрузки в outbox admin=%s", admin_id)


def _schedule_export(admin_id, today_only):
    context = _current_inbox_context()
    if context is not None:
        context["exports"].append((admin_id, today_only, context["inbox_id"]))
        return
    _submit_export(admin_id, today_only)


# ----------------- ДОЛГОВЕЧНАЯ ОЧЕРЕДЬ ВХОДЯЩИХ -----------------
def enqueue_incoming(user_id, peer_id, message_id, text, vk_date=None):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(
            "INSERT INTO bot_inbox (peer_id, user_id, vk_message_id, vk_date, text) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (peer_id, vk_message_id) DO NOTHING",
            (peer_id, user_id, message_id, int(vk_date or time.time()), text)
        )
        inserted = c.rowcount > 0
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_db(conn)
    _inbox_wake.set()
    return inserted

def enqueue_incoming_batch(entries):
    if not entries:
        return 0
    conn = get_db()
    try:
        c = conn.cursor()
        c.executemany(
            "INSERT INTO bot_inbox (peer_id, user_id, vk_message_id, vk_date, text) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (peer_id, vk_message_id) DO NOTHING",
            entries
        )
        inserted = max(c.rowcount, 0)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_db(conn)
    _inbox_wake.set()
    return inserted

# ----------------- ОБРАБОТКА НЕПРОЧИТАННЫХ -----------------

def process_unread_messages():
    enqueued = 0
    skipped = 0
    conversations = []
    offset = 0
    api = _get_message_vk()
    try:
        while True:
            result = api.messages.getConversations(
                filter='unread', count=200, offset=offset, extended=0
            )
            items = result.get('items', [])
            conversations.extend(items)
            if len(items) < 200:
                break
            offset += len(items)
    except Exception as e:
        log.exception("Ошибка getConversations")
        return

    for conv in conversations:
        conv_info = conv.get('conversation', {})
        peer_id = conv_info.get('peer', {}).get('id')
        unread_count = conv_info.get('unread_count', 0)
        if not peer_id or not unread_count:
            continue

        messages = []
        history_offset = 0
        failed = False
        while len(messages) < unread_count:
            count = min(200, unread_count - len(messages))
            try:
                history = api.messages.getHistory(
                    peer_id=peer_id, count=count, offset=history_offset
                )
            except Exception:
                log.exception("Ошибка getHistory peer=%s", peer_id)
                failed = True
                break
            batch = history.get('items', [])
            if not batch:
                failed = True
                break
            messages.extend(message for message in batch if message.get('from_id', 0) >= 0)
            skipped += sum(1 for message in batch if message.get('from_id', 0) < 0)
            history_offset += len(batch)
            if len(batch) < count and len(messages) < unread_count:
                failed = True
                break

        messages = messages[:unread_count]
        messages.reverse()
        pending = []
        for message in messages:
            text = (message.get('text') or '').strip()
            message_id = message.get('id')
            if not message_id:
                log.error("У непрочитанного сообщения нет id: peer=%s", peer_id)
                failed = True
                break
            pending.append((peer_id, peer_id, message_id, int(message.get('date') or time.time()), text))

        if len(messages) < unread_count:
            failed = True
        if pending and not failed:
            try:
                enqueued += enqueue_incoming_batch(pending)
            except Exception:
                log.exception("Не удалось сохранить историю peer=%s", peer_id)
                failed = True

        if not failed:
            try:
                api.messages.markAsRead(peer_id=peer_id)
            except Exception:
                log.exception("Ошибка markAsRead peer=%s", peer_id)

    log.info("В очередь добавлено сообщений: %s; пропущено: %s", enqueued, skipped)


def _unread_recovery_worker():
    while True:
        try:
            process_unread_messages()
        except Exception:
            log.exception("Ошибка периодического восстановления непрочитанных")
        time.sleep(60)

# ----------------- ОСНОВНАЯ ЛОГИКА -----------------

_USER_LOCKS = {}
_USER_LOCKS_GUARD = threading.Lock()

@contextmanager
def _lock_user(user_id):
    with _USER_LOCKS_GUARD:
        entry = _USER_LOCKS.get(user_id)
        if entry is None:
            entry = {"lock": threading.RLock(), "users": 0}
            _USER_LOCKS[user_id] = entry
        entry["users"] += 1
    waiting = time.monotonic()
    try:
        with entry["lock"]:
            yield time.monotonic() - waiting
    finally:
        with _USER_LOCKS_GUARD:
            entry["users"] -= 1
            if entry["users"] == 0 and _USER_LOCKS.get(user_id) is entry:
                del _USER_LOCKS[user_id]

def _serialize_user_messages(func):
    def wrapped(user_id, text):
        with _lock_user(user_id) as lock_wait:
            if lock_wait >= 0.05:
                log.warning(
                    "Ожидание блокировки пользователя peer=%s duration=%.3fs",
                    user_id, lock_wait
                )
            return func(user_id, text)
    return wrapped

@_serialize_user_messages
def handle_message(user_id, text):
    t0 = time.time()
    if not user_id:
        return
    text = (text or "").strip()
    if not text:
        step_index, uni_page, started = get_progress_cached(user_id)
        if started == 0:
            send_message(user_id, MESSAGES["welcome"], KB_START)
        elif started == 2 or step_index >= len(STEPS):
            send_message(user_id, MESSAGES["already_finished"], KB_RESTART)
        else:
            ask_step(user_id, STEPS[max(0, step_index)], uni_page)
        return
    text_lower = text.lower()

    if text_lower in ["/export", "/выгрузить"]:
        is_admin = user_id in ADMIN_IDS
        log.info("Команда выгрузки user=%s is_admin=%s", user_id, is_admin)
        if is_admin:
            send_message(user_id, "📊 Запрос на выгрузку принят. Подготавливаю файл…")
            _schedule_export(user_id, False)
        else:
            send_message(user_id, MESSAGES["admin_only"])
        return

    if text_lower in ["/export today", "/выгрузить сегодня", "/выгрузить_сегодня"]:
        is_admin = user_id in ADMIN_IDS
        log.info("Команда выгрузки за сегодня user=%s is_admin=%s", user_id, is_admin)
        if is_admin:
            send_message(user_id, "📊 Запрос на выгрузку за сегодня принят. Подготавливаю файл…")
            _schedule_export(user_id, True)
        else:
            send_message(user_id, MESSAGES["admin_only"])
        return

    if text_lower == "/restart":
        set_progress_cached(user_id, 0, 0, 0)
        send_message(user_id, "Анкета сброшена. Нажмите «Начать анкету».", KB_START)
        return

    if text == "🔄 Пройти заново":
        set_progress_cached(user_id, 0, 0, 0)
        send_message(user_id, MESSAGES["welcome"], KB_START)
        return

    t_db_start = time.time()
    step_index, uni_page, started = get_progress_cached(user_id)
    t_db_read = time.time() - t_db_start
    step_index = step_index if isinstance(step_index, int) else 0
    uni_page = uni_page if isinstance(uni_page, int) else 0
    started = started if isinstance(started, int) else 0

    if started == 0:
        if text == "Начать анкету":
            set_progress_cached(user_id, 0, 0, 1)
            ask_step(user_id, STEPS[0])
        else:
            send_message(user_id, MESSAGES["welcome"], KB_START)
        t_total = time.time() - t0
        log_msg(f"handle_message user={user_id} step=welcome duration={t_total:.3f}s db={t_db_read:.3f}s")
        return

    if started == 2 or step_index >= len(STEPS):
        send_message(user_id, MESSAGES["already_finished"], KB_RESTART)
        t_total = time.time() - t0
        log_msg(f"handle_message user={user_id} step=finished duration={t_total:.3f}s db={t_db_read:.3f}s")
        return

    step_key = STEPS[step_index]
    fio_text = None

    if step_key == "fio":
        ok, value = validate_fio(text)
        if ok:
            fio_text = value
            save_and_advance(user_id, "fio", value, step_index, 0, fio_text)
        else:
            send_message(user_id, value)
        t_total = time.time() - t0
        log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
        return

    if step_key == "institution":
        if text.lower() in ["далее", ">", "следующий"]:
            max_page = (len(UNIVERSITIES) - 1) // ITEMS_PER_PAGE
            if uni_page < max_page:
                set_progress_cached(user_id, step_index, uni_page + 1, 1)
                ask_university_page(user_id, uni_page + 1)
            else:
                send_message(user_id, "Это последняя страница.")
                ask_university_page(user_id, uni_page)
            t_total = time.time() - t0
            log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
            return
        elif text.lower() in ["назад", "<", "←"]:
            if uni_page > 0:
                set_progress_cached(user_id, step_index, uni_page - 1, 1)
                ask_university_page(user_id, uni_page - 1)
            else:
                send_message(user_id, "Это первая страница.")
                ask_university_page(user_id, uni_page)
            t_total = time.time() - t0
            log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
            return
        if text.isdigit():
            idx = int(text) - 1
            if 0 <= idx < len(UNIVERSITIES):
                save_and_advance(user_id, "institution", UNIVERSITIES[idx], step_index, uni_page)
                t_total = time.time() - t0
                log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
                return
        send_message(user_id, "Пожалуйста, введите номер учебного заведения из списка или используйте «далее» / «назад».")
        ask_university_page(user_id, uni_page)
        t_total = time.time() - t0
        log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
        return

    if step_key == "contacts":
        ok, value = validate_contacts(text)
        if ok:
            save_and_advance(user_id, "contacts", value, step_index)
        else:
            send_message(user_id, MESSAGES["invalid_contact"])
        t_total = time.time() - t0
        log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
        return

    if step_key in OPTIONS:
        opts = OPTIONS[step_key]
        if step_key == "consent":
            n = parse_single_number(text, len(opts))
            if n is None:
                send_message(user_id, MESSAGES["invalid_number"].format(len(opts)))
                t_total = time.time() - t0
                log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
                return
            is_consent = (n == 1)
            save_and_advance(user_id, "consent_status", is_consent, step_index)
            t_total = time.time() - t0
            log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
            return

        if step_key in MULTI_STEPS:
            nums = parse_multi_numbers(text, len(opts))
            if nums is None:
                send_message(user_id, MESSAGES["invalid_multi"].format(len(opts)))
                t_total = time.time() - t0
                log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
                return
            label = "; ".join(opts[n - 1] for n in nums)
            save_and_advance(user_id, STEP_TO_DB[step_key], label, step_index)
        else:
            n = parse_single_number(text, len(opts))
            if n is None:
                send_message(user_id, MESSAGES["invalid_number"].format(len(opts)))
                t_total = time.time() - t0
                log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
                return
            save_and_advance(user_id, STEP_TO_DB[step_key], opts[n - 1], step_index)
        t_total = time.time() - t0
        log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
        return

    if step_key not in ["post_plans", "help_needed"]:
        if len(text) < 2:
            send_message(user_id, "Пожалуйста, введите более развёрнутый ответ.")
            t_total = time.time() - t0
            log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")
            return

    save_and_advance(user_id, STEP_TO_DB[step_key], text, step_index)
    t_total = time.time() - t0
    log_msg(f"handle_message user={user_id} step={step_key} duration={t_total:.3f}s db={t_db_read:.3f}s")

# ==================== ОЧЕРЕДИ ОБРАБОТКИ ====================

MAX_INBOX_ATTEMPTS = 8
INBOX_WORKERS = 20
OUTBOX_WORKERS = 12
_inbox_executor = None
_outbox_executor = None
_executor_heavy = None
_inbox_slots = threading.BoundedSemaphore(INBOX_WORKERS)
_outbox_slots = threading.BoundedSemaphore(OUTBOX_WORKERS)


def _apply_progress_updates(context):
    if not context["progress_updates"]:
        return
    with cache_lock:
        progress_cache.update(context["progress_updates"])


def _retry_inbox(inbox_id, error):
    conn = None
    try:
        conn = db_pool.getconn()
        c = conn.cursor()
        c.execute(
            "SELECT attempts, peer_id FROM bot_inbox WHERE id=%s AND status='pending' FOR UPDATE",
            (inbox_id,)
        )
        row = c.fetchone()
        if row:
            attempts = row[0] + 1
            if attempts >= MAX_INBOX_ATTEMPTS:
                c.execute(
                    "UPDATE bot_inbox SET status='failed', attempts=%s, last_error=%s WHERE id=%s",
                    (attempts, str(error)[:2000], inbox_id)
                )
                c.execute(
                    "INSERT INTO bot_outbox "
                    "(inbox_id, sequence, peer_id, message, random_id) "
                    "VALUES (%s,0,%s,%s,%s) ON CONFLICT (inbox_id, sequence) DO NOTHING",
                    (inbox_id, row[1],
                     "Извините, не получилось обработать ваш ответ. Пожалуйста, отправьте его ещё раз.",
                     get_random_id())
                )
                conn.commit()
                _outbox_wake.set()
                log.error("inbox_id=%s marked failed after %s attempts", inbox_id, attempts)
            else:
                delay = min(60, 2 ** min(attempts, 6))
                c.execute(
                    "UPDATE bot_inbox SET attempts=%s, available_at=now() + (%s * interval '1 second'), "
                    "last_error=%s WHERE id=%s AND status='pending'",
                    (attempts, delay, str(error)[:2000], inbox_id)
                )
                conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        log.exception("Не удалось запланировать повтор inbox_id=%s", inbox_id)
    finally:
        if conn:
            db_pool.putconn(conn)
    _inbox_wake.set()


def _process_one_inbox():
    conn = None
    context = None
    inbox_id = None
    failure = None
    try:
        conn = db_pool.getconn()
        c = conn.cursor()
        c.execute("""
            SELECT m.id, m.user_id, m.text
            FROM bot_inbox m
            WHERE m.status='pending' AND m.available_at <= now()
              AND NOT EXISTS (
                  SELECT 1 FROM bot_inbox older
                  WHERE older.user_id=m.user_id
                    AND (older.vk_date < m.vk_date
                         OR (older.vk_date=m.vk_date AND older.vk_message_id<m.vk_message_id)
                         OR (older.vk_date=m.vk_date AND older.vk_message_id=m.vk_message_id AND older.id<m.id))
                    AND older.status='pending'
              )
            ORDER BY m.vk_date, m.vk_message_id, m.id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        """)
        row = c.fetchone()
        if not row:
            conn.rollback()
            return False

        inbox_id, user_id, text = row
        context = {
            "inbox_id": inbox_id,
            "borrowed_connection": _BorrowedConnection(conn),
            "progress_updates": {},
            "replies": [],
            "exports": []
        }
        _inbox_context.current = context
        with _lock_user(user_id):
            handle_message(user_id, text)

            c = conn.cursor()
            for sequence, reply in enumerate(context["replies"]):
                c.execute(
                    "INSERT INTO bot_outbox "
                    "(inbox_id, sequence, peer_id, message, keyboard, attachment, random_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (inbox_id, sequence) DO NOTHING",
                    (inbox_id, sequence, reply["peer_id"], reply["message"],
                     reply["keyboard"], reply["attachment"], get_random_id())
                )
            c.execute(
                "UPDATE bot_inbox SET status='done', last_error=NULL WHERE id=%s",
                (inbox_id,)
            )
            conn.commit()
            _apply_progress_updates(context)
            if context["replies"]:
                _outbox_wake.set()
            for export_job in context["exports"]:
                _submit_export(*export_job)
            log.info("inbox_done id=%s user=%s replies=%s", inbox_id, user_id, len(context["replies"]))
        return True
    except Exception as e:
        failure = e
        if conn:
            conn.rollback()
        log.exception("Ошибка обработки inbox_id=%s", inbox_id)
    finally:
        if hasattr(_inbox_context, "current"):
            del _inbox_context.current
        if conn:
            db_pool.putconn(conn)
    if inbox_id is not None and failure is not None:
        _retry_inbox(inbox_id, failure)
        return True
    return False


def _process_one_outbox():
    conn = None
    row = None
    try:
        conn = db_pool.getconn()
        c = conn.cursor()
        c.execute("""
            SELECT o.id, o.peer_id, o.message, o.keyboard, o.attachment, o.random_id, o.attempts
            FROM bot_outbox o
            WHERE o.status='pending' AND o.available_at <= now()
              AND NOT EXISTS (
                  SELECT 1 FROM bot_outbox older
                  WHERE older.peer_id=o.peer_id AND older.id<o.id
                    AND older.status IN ('pending','sending')
              )
            ORDER BY o.id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        """)
        row = c.fetchone()
        if not row:
            conn.rollback()
            return False
        outbox_id, peer_id, message, keyboard, attachment, random_id, attempts = row
        c.execute(
            "UPDATE bot_outbox SET status='sending', attempts=attempts+1, locked_at=now() WHERE id=%s",
            (outbox_id,)
        )
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        log.exception("Ошибка получения сообщения из outbox")
        return False
    finally:
        if conn:
            db_pool.putconn(conn)

    sent = send_message(peer_id, message, keyboard, attachment, random_id=random_id)
    conn = None
    try:
        conn = db_pool.getconn()
        c = conn.cursor()
        if sent is True:
            c.execute(
                "UPDATE bot_outbox SET status='sent', sent_at=now(), locked_at=NULL, last_error=NULL "
                "WHERE id=%s",
                (outbox_id,)
            )
        elif sent is None:
            c.execute(
                "UPDATE bot_outbox SET status='undeliverable', locked_at=NULL, "
                "last_error='VK ApiError 901: user has not allowed community messages' "
                "WHERE peer_id=%s AND status IN ('pending','sending')",
                (peer_id,)
            )
            log.warning(
                "Отклонены ожидающие сообщения peer=%s: VK ApiError 901; "
                "пользователь должен разрешить сообщения сообщества",
                peer_id
            )
        else:
            delay = min(60, 2 ** min(attempts + 1, 6))
            c.execute(
                "UPDATE bot_outbox SET status='pending', locked_at=NULL, "
                "available_at=now() + (%s * interval '1 second'), last_error='VK send failed' "
                "WHERE id=%s",
                (delay, outbox_id)
            )
            if attempts + 1 == 10 or (attempts + 1) % 100 == 0:
                log.error(
                    "VK send failed outbox_id=%s peer=%s attempts=%s; retry in %ss",
                    outbox_id, peer_id, attempts + 1, delay
                )
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        log.exception("Ошибка обновления outbox_id=%s", outbox_id)
        return True
    finally:
        if conn:
            db_pool.putconn(conn)
    _outbox_wake.set()
    return True


def _count_ready_inbox():
    conn = db_pool.getconn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT count(*) FROM bot_inbox m
            WHERE m.status='pending' AND m.available_at <= now()
              AND NOT EXISTS (
                  SELECT 1 FROM bot_inbox older
                  WHERE older.user_id=m.user_id
                    AND (older.vk_date < m.vk_date
                         OR (older.vk_date=m.vk_date AND older.vk_message_id<m.vk_message_id)
                         OR (older.vk_date=m.vk_date AND older.vk_message_id=m.vk_message_id AND older.id<m.id))
                    AND older.status='pending'
              )
        """)
        return c.fetchone()[0]
    finally:
        conn.rollback()
        db_pool.putconn(conn)


def _count_ready_outbox():
    conn = db_pool.getconn()
    try:
        c = conn.cursor()
        c.execute(
            "UPDATE bot_outbox SET status='pending', locked_at=NULL, available_at=now(), "
            "last_error='Recovered stale sending lease' "
            "WHERE status='sending' AND locked_at < now() - interval '2 minutes'"
        )
        c.execute("""
            SELECT count(*) FROM bot_outbox o
            WHERE o.status='pending' AND o.available_at <= now()
              AND NOT EXISTS (
                  SELECT 1 FROM bot_outbox older
                  WHERE older.peer_id=o.peer_id AND older.id<o.id
                    AND older.status IN ('pending','sending')
              )
        """)
        count = c.fetchone()[0]
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)


def _queue_task_done(future, slots, wake, queue_name):
    try:
        if future.result():
            wake.set()
    except Exception:
        log.exception("Необработанная ошибка worker %s", queue_name)
    finally:
        slots.release()


def _queue_dispatcher(wake, slots, executor, counter, worker, queue_name):
    while True:
        # Clear before checking the queue so a concurrent wake-up is never lost.
        wake.clear()
        try:
            pending = counter()
        except Exception:
            log.exception("Ошибка проверки очереди %s", queue_name)
            wake.wait(timeout=1.0)
            continue
        while pending > 0 and slots.acquire(blocking=False):
            try:
                future = executor.submit(worker)
            except Exception:
                slots.release()
                log.exception("Не удалось запустить worker %s", queue_name)
                break
            future.add_done_callback(
                lambda done, s=slots, w=wake, n=queue_name: _queue_task_done(done, s, w, n)
            )
            pending -= 1
        wake.wait(timeout=1.0)

# ==================== FASTAPI ====================

app = FastAPI()
_executor_heavy = None
_inbox_executor = None
_outbox_executor = None

@app.on_event("startup")
async def startup_event():
    global _inbox_executor, _outbox_executor, _executor_heavy
    _inbox_executor = concurrent.futures.ThreadPoolExecutor(max_workers=INBOX_WORKERS)
    _outbox_executor = concurrent.futures.ThreadPoolExecutor(max_workers=OUTBOX_WORKERS)
    _executor_heavy = concurrent.futures.ThreadPoolExecutor(max_workers=3)
    init_db_pool()
    init_db()
    preload_progress_cache()
    threading.Thread(
        target=_queue_dispatcher,
        args=(_inbox_wake, _inbox_slots, _inbox_executor, _count_ready_inbox, _process_one_inbox, "inbox"),
        daemon=True
    ).start()
    threading.Thread(
        target=_queue_dispatcher,
        args=(_outbox_wake, _outbox_slots, _outbox_executor, _count_ready_outbox, _process_one_outbox, "outbox"),
        daemon=True
    ).start()
    _inbox_wake.set()
    _outbox_wake.set()
    log_msg("Бот запущен; очереди inbox/outbox готовы.")
    threading.Thread(target=_unread_recovery_worker, daemon=True).start()

@app.post("/")
async def vk_callback(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_type = data.get("type")
    if not event_type:
        raise HTTPException(status_code=400, detail="No event type")

    if event_type == "confirmation":
        return PlainTextResponse(CALLBACK_CONFIRM_TOKEN)

    if event_type == "message_new":
        obj = data.get("object", {})
        msg = obj.get("message", {})
        user_id = msg.get("from_id") or msg.get("peer_id")
        peer_id = msg.get("peer_id") or user_id
        message_id = msg.get("id") or msg.get("conversation_message_id")
        text = (msg.get("text") or "").strip()

        if not user_id:
            return PlainTextResponse("ok")
        if not message_id:
            raise HTTPException(status_code=400, detail="Message ID is required")

        try:
            await asyncio.to_thread(
                enqueue_incoming, user_id, peer_id, message_id, text, msg.get("date")
            )
        except Exception:
            log.exception("Не удалось сохранить callback peer=%s id=%s", peer_id, message_id)
            raise HTTPException(status_code=503, detail="Message queue unavailable")
        return PlainTextResponse("ok")

    return PlainTextResponse("ok")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
