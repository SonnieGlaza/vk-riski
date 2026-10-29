#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Разбор «какой работы проведено» из файла или текста и привязка строк к студентам.

Модуль намеренно не знает ни про VK, ни про PostgreSQL: на входе — байты .xlsx/.csv
или обычный текст, на выходе — строки, привязанные к user_id из таблицы `answers`.
Поэтому логику (разбор файла, поиск студента по ФИО, отчёт о неоднозначностях) можно
проверить тестом без базы — см. `tests/test_work_import.py`.

Бот (`bot.py`) делает только «грязную» часть: скачивает вложение из сообщения,
показывает предпросмотр, спрашивает подтверждение и пишет историю в
`outreach_actions` — оттуда её читает колонка «Какая работа проведена» в выгрузках.

Ожидаемые колонки в файле (названия распознаются, порядок любой):

    ФИО | Какая работа проведена | [Контакты] | [Учебное заведение] | [Дата работы]

Обязательны только ФИО и работа. Если контакты или заведение есть — они помогают
развести однофамильцев и найти человека, чьё ФИО в файле записано иначе.

Текстовый режим (админ копирует две колонки из Excel и вставляет в VK):

    Иванов Иван Иванович | отправлено письмо в техникум
    Петрова Мария Петровна; позвонили, пригласили на ярмарку

Разделитель — табуляция (как при копировании из Excel), «|» или «;».
"""
from __future__ import annotations

import csv
import io
import re
from datetime import date, datetime

# --- Нормализация -----------------------------------------------------------------

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_PHONE_RE = re.compile(r"\+?\d[\d\s()\-]{6,}\d")


def normalize_fio(value):
    """ФИО к сравнимому виду: «ё»→«е», без регистра, знаков и лишних пробелов."""
    text = str(value or "").replace("ё", "е").replace("Ё", "Е").casefold()
    text = re.sub(r"[^0-9a-zа-я]+", " ", text)
    return " ".join(text.split())


def normalize_institution(value):
    """Название заведения к сравнимому виду: без кавычек, точек и двойных пробелов."""
    text = str(value or "").replace("ё", "е").replace("Ё", "Е")
    text = re.sub(r"(?<=[а-яеА-ЯЕa-zA-Z])(?=[А-ЯЁA-Z])", " ", text)
    text = text.casefold().replace("им.", "имени")
    text = re.sub(r"[«»\"'()\.,;:!?]", " ", text)
    return " ".join(text.split())


def digits_of(value):
    return re.sub(r"\D", "", str(value or ""))


def phone_key(value):
    """Последние 10 цифр номера — «+7…», «8…» и «7912…» дают один ключ."""
    digits = digits_of(value)
    return digits[-10:] if len(digits) >= 10 else ""


def emails_of(value):
    return [m.group(0).casefold() for m in _EMAIL_RE.finditer(str(value or ""))]


def phones_of(value):
    return [key for key in (phone_key(m.group(0)) for m in _PHONE_RE.finditer(str(value or ""))) if key]


def parse_date(value):
    """Дата работы в виде «ГГГГ-ММ-ДД» или None, если разобрать не удалось."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y", "%d/%m/%Y", "%d-%m-%Y",
                "%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    match = re.match(r"^(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})", text)
    if match:
        day, month, year = (int(part) for part in match.groups())
        if year < 100:
            year += 2000
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None
    return None


# --- Колонки ----------------------------------------------------------------------

# Порядок важен: первое подходящее название выигрывает.
COLUMN_ALIASES = {
    "fio": (
        "фио", "ф.и.о.", "ф.и.о", "фио студента", "фамилия имя отчество",
        "студент", "обучающийся", "name",
    ),
    "action": (
        "какая работа проведена", "работа проведена", "работа с обучающимся",
        "что сделано", "что сделали", "действие", "действия", "комментарий",
        "примечание", "результат", "отметка о работе",
    ),
    "contacts": (
        "контакты", "контакт", "телефон", "тел.", "тел", "мобильный",
        "почта", "email", "e-mail", "e mail",
    ),
    "institution": (
        "учебное заведение", "заведение", "образовательная организация",
        "вуз", "ссуз", "колледж", "техникум",
    ),
    "date": (
        "дата работы", "дата выполнения", "дата контакта", "когда", "дата",
    ),
}

# Колонки, которые нельзя принимать за «дату»: в выгрузке бота это дата анкеты.
DATE_STOPWORDS = ("дата заполнения", "дата рождения", "дата анкетирования")


def _norm_header(value):
    return re.sub(r"\s+", " ", str(value or "").strip().casefold()).replace("ё", "е")


def detect_columns(headers):
    """Какие поля нашлись в шапке: {field: индекс}. Пустая шапка — тоже результат."""
    normalized = [_norm_header(h) for h in headers]
    found = {}
    for field, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            for idx, header in enumerate(normalized):
                if idx in found.values():
                    continue
                if header == alias:
                    found[field] = idx
                    break
            if field in found:
                break
    # Второй проход мягче: «Какая работа проведена с обучающимся» и т. п.
    for field, aliases in COLUMN_ALIASES.items():
        if field in found:
            continue
        for alias in aliases:
            if len(alias) < 6:
                continue
            for idx, header in enumerate(normalized):
                if idx in found.values():
                    continue
                if header.startswith(alias):
                    found[field] = idx
                    break
            if field in found:
                break
    date_idx = found.get("date")
    if date_idx is not None and normalized[date_idx].startswith(DATE_STOPWORDS):
        found.pop("date", None)
    return found


def _text(value):
    return "" if value is None else str(value).strip()


def _row_from_values(values, columns):
    def pick(field):
        idx = columns.get(field)
        if idx is None or idx >= len(values):
            return ""
        return _text(values[idx])

    return {
        "fio": pick("fio"),
        "action": pick("action"),
        "contacts": pick("contacts"),
        "institution": pick("institution"),
        "date": pick("date"),
    }


def _looks_like_header(values, columns):
    """Шапка — строка, в которой распознано минимум два поля (или ФИО и работа)."""
    if not columns:
        return False
    hits = 0
    for field, idx in columns.items():
        if idx >= len(values):
            continue
        header = _norm_header(values[idx])
        if any(alias == header or header.startswith(alias) for alias in COLUMN_ALIASES[field]):
            hits += 1
    return hits >= 2


def rows_from_grid(grid, header_scan=5):
    """Строки из таблицы (список списков). Шапка ищется в первых строках.

    Возвращает (rows, meta). Если работа не заполнена, строка всё равно попадает
    в результат со статусом «empty_action» — чтобы в отчёте было видно, сколько
    строк осталось без работы.
    """
    rows = []
    header_row = None
    columns = {}
    for offset, values in enumerate(grid[:header_scan]):
        candidate = detect_columns(values)
        if _looks_like_header(values, candidate):
            header_row = offset
            columns = candidate
            break
    if header_row is None:
        # Шапки нет: считаем, что первая колонка — ФИО, вторая — работа.
        columns = {"fio": 0, "action": 1}
        header_row = -1
    for values in grid[header_row + 1:]:
        if values is None or all(_text(v) == "" for v in values):
            continue
        rows.append(_row_from_values(list(values), columns))
    meta = {
        "columns": {field: idx + 1 for field, idx in columns.items()},
        "header_row": header_row + 1 if header_row >= 0 else None,
        "rows_total": len(rows),
        "actions_filled": sum(1 for row in rows if row["action"]),
    }
    return rows, meta


# --- Файлы ------------------------------------------------------------------------

def rows_from_xlsx(data):
    """Строки из первого листа, где есть заполненная колонка «какая работа проведена»."""
    import openpyxl

    source = io.BytesIO(data) if isinstance(data, (bytes, bytearray)) else data
    workbook = openpyxl.load_workbook(source, read_only=True, data_only=True)
    try:
        best = None
        for ws in workbook.worksheets:
            grid = [list(row) for row in ws.iter_rows(values_only=True)]
            rows, meta = rows_from_grid(grid)
            if not rows:
                continue
            if meta["actions_filled"]:
                return rows, dict(meta, sheet=ws.title)
            if best is None:
                best = (rows, dict(meta, sheet=ws.title))
        if best is not None:
            return best
        return [], {"columns": {}, "header_row": None, "rows_total": 0, "actions_filled": 0,
                    "sheet": None}
    finally:
        workbook.close()


def rows_from_csv(data):
    text = data.decode("utf-8-sig", errors="replace") if isinstance(data, (bytes, bytearray)) else str(data)
    sample = text[:2000]
    delimiter = ";" if sample.count(";") > sample.count(",") else ","
    grid = [row for row in csv.reader(io.StringIO(text), delimiter=delimiter)]
    return rows_from_grid(grid)


SEPARATORS = ("\t", "|", ";")
# По этим словам понимаем, что средняя колонка — не контакт, а заведение.
INSTITUTION_MARKERS = (
    "бпоу", "апоу", "гбоу", "фгбоу", "техникум", "колледж", "училище",
    "университет", "институт", "академия", "лицей", "филиал",
)


def looks_like_institution(value):
    text = str(value or "").casefold()
    return any(marker in text for marker in INSTITUTION_MARKERS)


def rows_from_text(text):
    """Строки из сообщения: «ФИО<разделитель>что сделано».

    Понимает и три колонки — «ФИО | заведение | работа» или
    «ФИО | телефон или почта | работа»: заведение помогает развести однофамильцев,
    контакты — найти человека, чьё ФИО в файле записано иначе.
    """
    rows = []
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = None
        for separator in SEPARATORS:
            if separator in line:
                parts = [part.strip() for part in line.split(separator)]
                break
        if parts is None:
            # Двоеточие принимаем, только если слева похоже на ФИО (2–4 слова).
            if ":" in line:
                left, right = line.split(":", 1)
                if 2 <= len(left.split()) <= 4:
                    parts = [left.strip(), right.strip()]
        if parts is None:
            continue
        parts = [part for part in parts if part]
        if len(parts) < 2:
            continue
        contacts, institution = "", ""
        for part in parts[1:-1]:
            if looks_like_institution(part):
                institution = f"{institution} {part}".strip()
            else:
                contacts = f"{contacts} {part}".strip()
        rows.append({
            "fio": parts[0],
            "contacts": contacts,
            "institution": institution,
            "action": parts[-1],
            "date": "",
        })
    return rows


def parse_file(filename, data):
    """Разобрать файл по расширению. Возвращает (rows, meta)."""
    name = str(filename or "").lower()
    if name.endswith((".xlsx", ".xlsm")):
        return rows_from_xlsx(data)
    if name.endswith(".csv"):
        return rows_from_csv(data)
    if name.endswith(".txt"):
        rows = rows_from_text(data.decode("utf-8-sig", errors="replace") if isinstance(data, (bytes, bytearray)) else data)
        return rows, {"columns": {}, "header_row": None, "rows_total": len(rows),
                      "actions_filled": sum(1 for row in rows if row["action"]), "sheet": None}
    if name.endswith(".xls"):
        raise ValueError("Формат .xls не поддерживается: сохраните файл как .xlsx или .csv")
    raise ValueError("Поддерживаются файлы .xlsx, .xlsm, .csv и .txt")


# --- Привязка строк к анкетам -----------------------------------------------------

STATUS_TITLES = {
    "ok": "нашли студента",
    "ok_contacts": "нашли по контактам",
    "ok_short_fio": "нашли по короткому ФИО",
    "ok_institution": "нашли по заведению",
    "ambiguous": "однофамильцы — не угадываю",
    "not_found": "студент не найден",
    "empty_action": "работа не заполнена",
    "no_student": "не указано, о ком речь",
}


def build_index(candidates):
    """Индексы анкет: по ФИО, по телефону и по почте."""
    index = {"by_fio": {}, "by_phone": {}, "by_email": {}}
    for candidate in candidates or []:
        key = normalize_fio(candidate.get("fio"))
        if key:
            index["by_fio"].setdefault(key, []).append(candidate)
        phone = phone_key(candidate.get("contacts"))
        if phone:
            index["by_phone"].setdefault(phone, []).append(candidate)
        for email in emails_of(candidate.get("contacts")):
            index["by_email"].setdefault(email, []).append(candidate)
    return index


def _short_fio_matches(index, key):
    """Уникальное совпадение по началу ФИО: «Иванов Иван» → «Иванов Иван Иванович»."""
    if len(key.split()) < 2:
        return []
    hits = []
    for candidate_key, items in index["by_fio"].items():
        if candidate_key.startswith(key + " "):
            hits.extend(items)
    return hits


def _pick_by_institution(candidates, institution):
    """Из однофамильцев выбираем того, чьё заведение совпадает с указанным в файле."""
    if not institution:
        return []
    wanted = normalize_institution(institution)
    if not wanted:
        return []
    exact = [c for c in candidates if normalize_institution(c.get("institution")) == wanted]
    if len(exact) == 1:
        return exact
    soft = []
    for candidate in candidates:
        have = normalize_institution(candidate.get("institution"))
        if have and (wanted in have or have in wanted):
            soft.append(candidate)
    return soft if len(soft) == 1 else []


def match_rows(rows, candidates):
    """Привязать строки к анкетам. Возвращает копии строк со статусом и user_id.

    Ничего не угадывает при неоднозначности: если под ФИО подходят несколько
    человек и развести их нечем, строка получает статус «ambiguous» и в базу не
    попадёт — её видно в предпросмотре.
    """
    index = build_index(candidates)
    matched = []
    for row in rows or []:
        item = dict(row)
        item.setdefault("contacts", "")
        item.setdefault("institution", "")
        item["user_id"] = None
        item["matched_fio"] = ""
        item["matched_institution"] = ""
        action = _text(item.get("action"))
        fio = _text(item.get("fio"))
        if not action:
            item["status"] = "empty_action"
            matched.append(item)
            continue
        if not fio and not phone_key(item["contacts"]) and not emails_of(item["contacts"]):
            item["status"] = "no_student"
            matched.append(item)
            continue

        found = []
        how = "ok"
        key = normalize_fio(fio)
        if key:
            found = list(index["by_fio"].get(key) or [])
        if not found and key:
            short = _short_fio_matches(index, key)
            if short:
                found = short
                how = "ok_short_fio"
        if not found:
            phone = phone_key(item["contacts"])
            if phone:
                found = list(index["by_phone"].get(phone) or [])
                if found:
                    how = "ok_contacts"
        if not found:
            for email in emails_of(item["contacts"]):
                found = list(index["by_email"].get(email) or [])
                if found:
                    how = "ok_contacts"
                    break
        if len(found) > 1:
            picked = _pick_by_institution(found, item["institution"])
            if picked:
                found = picked
                how = "ok_institution"
        if not found:
            item["status"] = "not_found"
        elif len(found) > 1:
            item["status"] = "ambiguous"
            item["candidates"] = [
                " — ".join(part for part in (c.get("fio"), c.get("institution")) if part)
                for c in found[:5]
            ]
        else:
            item["status"] = how
            item["user_id"] = found[0].get("user_id")
            item["matched_fio"] = found[0].get("fio") or ""
            item["matched_institution"] = found[0].get("institution") or ""
        matched.append(item)
    return matched


def count_statuses(rows):
    counts = {}
    for row in rows or []:
        status = row.get("status") or "unknown"
        counts[status] = counts.get(status, 0) + 1
    return counts


def ready_rows(rows):
    """Только те строки, которые можно записывать в историю работы."""
    return [
        row for row in rows or []
        if str(row.get("status") or "").startswith("ok") and row.get("user_id") and _text(row.get("action"))
    ]
