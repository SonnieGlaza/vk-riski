"""MAX Bot — анкета для опроса студентов (аналог VK-бота).

Работает параллельно с VK-ботом, использует ту же базу данных PostgreSQL.
Для различения платформ: VK user_id > 0, MAX chat_id хранится как отрицательное число.

Установка:
    pip install maxapi aiohttp psycopg2-binary openpyxl

Запуск:
    MAX_TOKEN=токен DATABASE_URL=url ADMIN_IDS=123,456 python max_bot.py
"""

import os
import re
import json
import asyncio
import logging
import io
import csv
import psycopg2
from psycopg2.extras import RealDictCursor

import aiohttp
from maxapi import Bot, Dispatcher, F
from maxapi.types import MessageCreated, BotStarted, Command

logging.basicConfig(level=logging.INFO, format="%(asctime)s [MAX] %(message)s")

# ================= НАСТРОЙКИ ИЗ ENV =================
MAX_TOKEN = os.getenv("MAX_TOKEN", "")
ADMIN_IDS = set(map(int, os.getenv("ADMIN_IDS", "").split(","))) if os.getenv("ADMIN_IDS") else set()
DATABASE_URL = os.getenv("DATABASE_URL", "")
MAX_API_BASE = "https://platform-api2.max.ru"

if not MAX_TOKEN or not DATABASE_URL:
    raise ValueError("Не заданы переменные окружения MAX_TOKEN или DATABASE_URL")

# --- РЕГЕКСЫ ---
PHONE_PATTERN = re.compile(r'^\+?[78]?[\s\-]?$?\d{3}$?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}$')
EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')

# --- ВСПОМОГАТЕЛЬНЫЕ ---
def format_numbered_list(items, start_from=1, truncate=True):
    lines = []
    for i, item in enumerate(items, start=start_from):
        if truncate and len(item) > 80:
            item = item[:77] + "…"
        lines.append(f"{i} — {item}")
    return "\n".join(lines)

# --- ИДЕНТИФИКАТОР В БД ---
# MAX chat_id хранится как отрицательное число, чтобы не конфликтовать с VK user_id
def to_db_id(chat_id):
    return -int(chat_id)

# ----------------- БАЗА ДАННЫХ -----------------
def get_db():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS answers (
            user_id BIGINT PRIMARY KEY,
            fio TEXT, institution TEXT, specialty TEXT, study_group TEXT,
            course TEXT, form_of_study TEXT, contacts TEXT,
            employment_status TEXT, target_contract TEXT, experience TEXT,
            practice_eval TEXT, events TEXT, resume_status TEXT,
            interview_training TEXT, special_status TEXT, military TEXT,
            maternity TEXT, graduate TEXT, post_plans TEXT, help_needed TEXT
        )
    """)
    c.execute("ALTER TABLE answers ADD COLUMN IF NOT EXISTS consent_status BOOLEAN DEFAULT FALSE")
    c.execute("""
        CREATE TABLE IF NOT EXISTS progress (
            user_id BIGINT PRIMARY KEY,
            step_index INTEGER DEFAULT 0,
            uni_page INTEGER DEFAULT 0,
            started INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()

def get_progress(db_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT step_index, uni_page, started FROM progress WHERE user_id=%s", (db_id,))
    row = c.fetchone()
    conn.close()
    return (row[0], row[1], row[2]) if row else (0, 0, 0)

def set_progress(db_id, step_index, uni_page=0, started=1):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT INTO progress (user_id, step_index, uni_page, started) VALUES (%s,%s,%s,%s) "
        "ON CONFLICT (user_id) DO UPDATE SET step_index=%s, uni_page=%s, started=%s",
        (db_id, step_index, uni_page, started, step_index, uni_page, started)
    )
    conn.commit()
    conn.close()
