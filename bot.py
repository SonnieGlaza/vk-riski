import os
import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.utils import get_random_id
import re
import csv
import io
import json
import time
import psycopg2
from psycopg2.extras import RealDictCursor

# ================= НАСТРОЙКИ =================
TOKEN = os.environ.get("VK_TOKEN", "")
ADMIN_IDS_RAW = os.environ.get("ADMIN_IDS", "")
ADMIN_IDS = {int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip()}
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5432/db")
# ==============================================

vk_session = vk_api.VkApi(token=TOKEN)
vk = vk_session.get_api()
longpoll = VkLongPoll(vk_session)

try:
    PHONE_PATTERN = re.compile(r'^\+?[78]?[\s\-]?$?\d{3}$?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}$')
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')
except re.error as e:
    print("ОШИБКА В REGEX:", e)
    raise

# ----------------- БАЗА ДАННЫХ -----------------
def get_db():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS answers (
            user_id BIGINT PRIMARY KEY,
            fio TEXT, institution TEXT, specialty TEXT, study_group TEXT,
            course TEXT, form_of_study TEXT, contacts TEXT,
            employment_status TEXT, target_contract TEXT, experience TEXT,
            practice_eval TEXT, events TEXT, resume_status TEXT,
            interview_training TEXT, special_status TEXT, military TEXT,
            maternity TEXT, graduate TEXT, post_plans TEXT, help_needed TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS progress (
            user_id BIGINT PRIMARY KEY,
            step_index INTEGER DEFAULT 0,
            uni_page INTEGER DEFAULT 0,
            started INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

def get_progress(user_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT step_index, uni_page, started FROM progress WHERE user_id=%s", (user_id,))
    row = c.fetchone()
    conn.close()
    return (row[0], row[1], row[2]) if row else (0, 0, 0)

def set_progress(user_id, step_index, uni_page=0, started=1):
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT INTO progress (user_id, step_index, uni_page, started) VALUES (%s,%s,%s,%s) "
        "ON CONFLICT (user_id) DO UPDATE SET step_index=%s, uni_page=%s, started=%s",
        (user_id, step_index, uni_page, started, step_index, uni_page, started)
    )
    conn.commit()
    conn.close()

def save_answer(user_id, field, value):
    cols = ["fio","institution","specialty","study_group","course","form_of_study",
            "contacts","employment_status","target_contract","experience",
            "practice_eval","events","resume_status","interview_training",
            "special_status","military","maternity","graduate","post_plans","help_needed"]
    if field not in cols:
        return
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO answers (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING", (user_id,))
    c.execute('UPDATE answers SET ' + field + '=%s WHERE user_id=%s', (value, user_id))
    conn.commit()
    conn.close()

# ----------------- ВУЗы -----------------
UNIVERSITIES = [
    "БПОУ УР «Воткинский промышленный техникум»",
    "БПОУ УР «Воткинский музыкально-педагогический колледж имени П.И. Чайковского»",
    "БПОУ УР «Воткинский машиностроительный техникум имени В.Г.Садовникова»",
    "АПОУ УР «Глазовский аграрно-промышленный техникум»",
    "БПОУ УР «Глазовский технический колледж»",
    "БПОУ «Дебесский политехникум»",
    "БПОУР «Игринский политехнический техникум»",
    "БПОУ УР «Ижевский торгово-экономический техникум»",
    "БПОУ УР «Ижевский монтажный техникум»",
    "БПОУ «Ижевский агростроительный техникум»",
    "ЧПОО «Нефтяной техникум»",
    "БПОУ УР «Ижевский политехнический колледж»",
    "БПОУ УР «Ижевский промышленно-экономический колледж»",
    "БПОУ УР «Ижевский машиностроительный техникум им. С.Н. Борина»",
    "БПОУ УР «Радиомеханический техникум имени В.А. Шутова»",
    "АПОУ УР «Экономико-технологический колледж»",
    "БПОУ УР «Асановский аграрно-технический техникум»",
    "АПОУ УР «Топливно-энергетический колледж»",
    "БПОУ УР «Ижевский техникум индустрии питания»",
    "КПОУ УР «Удмуртский республиканский колледж культуры»",
    "АНПОО «Международный Восточно-Европейский колледж»",
    "АПОУ УР «Техникум радиоэлектроники и информационных технологий им. А.В. Воскресенского»",
    "АПОУ УР «Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной»",
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
    "БПОУ УР «Ижевский индустриальный техникум имени Е.Ф. Драгунова»",
    "БПОУ УР «Глазовский политехнический колледж»",
    "Минюст", "УдГУ", "УдГАУ", "ИжГТУ",
    "БПОУ УР «Ижевский автотранспортный техникум»",
    "ГИПУ",
    "Сарапульский техникум машиностроения и информационных технологий"
]
ITEMS_PER_PAGE = 10

# ----------------- ШАГИ -----------------
STEPS = [
    "fio", "institution", "specialty", "study_group",
    "course", "form_of_study", "contacts",
    "employment_status", "target_contract", "experience",
    "practice_eval", "events", "resume_status",
    "interview_training", "special_status", "military",
    "maternity", "graduate", "post_plans", "help_needed"
]

STEP_TO_DB = {s: s for s in STEPS}

QUESTIONS = {
    "fio": "Пожалуйста, укажите ваши фамилию, имя и отчество полностью:",
    "institution": "Выберите ваше учебное заведение из списка:",
    "specialty": "Укажите вашу специальность обучения:",
    "study_group": "Укажите номер вашей учебной группы:",
    "course": "Выберите ваш курс обучения:",
    "form_of_study": "Выберите форму обучения:",
    "contacts": "Укажите контактные данные — телефон или e-mail (например: +79991234567 или student@mail.ru):",
    "employment_status": "Ваш статус занятости прямо сейчас.\nВыберите один вариант, указав его номер:",
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
    "post_plans": "Ваши планы после выпуска.\nВыберите один или несколько вариантов, указав их номера через запятую (например: 1, 3):",
    "help_needed": "Какую помощь от Кадрового центра «Работа России» вы бы считали наиболее полезной?\nВыберите все подходящие варианты, указав их номера через запятую (например: 1, 2, 4):"
}

OPTIONS = {
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
    "target_contract": [
        "да, договор о целевом обучении заключён",
        "нет, договора о целевом обучении нет"
    ],
    "experience": [
        "да, есть опыт работы / оплачиваемой стажировки по основной или близкой специальности",
        "есть опыт работы только вне специальности обучения",
        "нет, опыта работы и оплачиваемых стажировок пока не было"
    ],
    "practice_eval": [
        "скорее доволен(льна) или полностью доволен(льна)",
        "скорее не доволен(льна) / совсем не доволен(льна) результатами практик"
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
    "interview_training": [
        "да, проходил(а) одно или несколько таких мероприятий",
        "пока не проходил(а)"
    ],
    "special_status": [
        "да, имею группу инвалидности",
        "отношусь к категории детей-сирот и детей, оставшихся без попечения родителей",
        "планирую переезд в другой регион / страну после окончания обучения",
        "ничего из вышеперечисленного"
    ],
    "military": [
        "да, планируется призыв",
        "нет / не подлежу призыву / вопрос уже решён (служба пройдена и др.)"
    ],
    "maternity": ["да, планирую", "пока не планирую"],
    "graduate": [
        "да, я учусь на выпускном курсе",
        "нет, я не на выпускном курсе"
    ],
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

# Только эти шаги — множественный выбор через запятую
MULTI_STEPS = ["post_plans", "help_needed"]

MESSAGES = {
    "welcome": (
        "👋 Здравствуйте! Я бот для оценки рисков нетрудоустройства студентов.\n\n"
        "Я задам несколько коротких вопросов о вашем обучении и занятости. "
        "Это займёт примерно 5–7 минут. Ответы помогут подобрать для вас "
        "подходящие вакансии, стажировки и поддержку от центра занятости.\n\n"
        "Нажмите кнопку «Начать анкету», чтобы приступить."
    ),
    "invalid_contact": (
        "Не удалось распознать контакт. Пожалуйста, введите:\n\n"
        "• Номер телефона в формате +79991234567 или 89991234567\n"
        "или\n"
        "• Адрес электронной почты в формате example@mail.ru"
    ),
    "invalid_choice": "Пожалуйста, выберите один из вариантов, нажав на кнопку ниже.",
    "invalid_number": "Пожалуйста, введите номер от 1 до {}.",
    "invalid_multi": "Пожалуйста, укажите номера вариантов через запятую (например: 1, 3, 5). Проверьте, что номера от 1 до {}.",
    "no_data": "Пока нет собранных анкет для выгрузки.",
    "admin_only": "Эта команда доступна только администраторам.",
    "finished": (
        "✅ Спасибо! Анкета заполнена.\n\n"
        "Если у вас возникнут вопросы, вы всегда можете написать сюда ещё раз."
    ),
    "already_finished": (
        "✅ Вы уже заполнили анкету ранее. Если хотите пройти заново, "
        "напишите команду /restart."
    )
}

# ----------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ -----------------

def send_message(user_id, message, keyboard=None, attachment=None):
    try:
        params = {
            "peer_id": user_id,
            "message": message,
            "random_id": get_random_id()
        }
        if keyboard:
            params["keyboard"] = keyboard
        if attachment:
            params["attachment"] = attachment
        vk.messages.send(**params)
    except Exception as e:
        print(f"Ошибка отправки: {e}")

def all_options_fit(options, max_len=40):
    """Проверяет, влезают ли все варианты в кнопку ВК (лимит 40 символов)."""
    return all(len(opt) <= max_len for opt in options)

def kb_start():
    return json.dumps({
        "one_time": False,
        "buttons": [[{
            "action": {"type": "text", "label": "Начать анкету"},
            "color": "positive"
        }]]
    })

def kb_options(step_key):
    opts = OPTIONS[step_key]
    rows = []
    for i in range(0, len(opts), 2):
        row = []
        row.append({"action": {"type": "text", "label": opts[i]}, "color": "primary"})
        if i + 1 < len(opts):
            row.append({"action": {"type": "text", "label": opts[i+1]}, "color": "primary"})
        rows.append(row)
    return json.dumps({"one_time": False, "buttons": rows})

def format_numbered_list(items, start_from=1):
    lines = []
    for i, item in enumerate(items, start=start_from):
        lines.append(f"{i} — {item}")
    return "\n".join(lines)

def validate_contact(text):
    text = text.strip()
    if PHONE_PATTERN.match(text):
        digits = re.sub(r'\D', '', text)
        if len(digits) == 11 and digits.startswith('8'):
            digits = '7' + digits[1:]
        elif len(digits) == 10:
            digits = '7' + digits
        if len(digits) == 11:
            return True, '+7' + digits[-10:]
    if EMAIL_PATTERN.match(text):
        return True, text.lower()
    return False, None

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

def ask_step(user_id, step_key, uni_page=0):
    if step_key == "institution":
        ask_university_page(user_id, uni_page)
    elif step_key in OPTIONS and all_options_fit(OPTIONS[step_key]):
        send_message(user_id, QUESTIONS[step_key], kb_options(step_key))
    elif step_key in OPTIONS:
        opts = OPTIONS[step_key]
        list_text = format_numbered_list(opts)
        if step_key in MULTI_STEPS:
            hint = "Напишите номера выбранных вариантов через запятую (например: 1, 3)."
        else:
            hint = "Напишите номер выбранного варианта (например: 1)."
        message = f"{QUESTIONS[step_key]}\n\n{list_text}\n\n{hint}"
        send_message(user_id, message)
    else:
        send_message(user_id, QUESTIONS[step_key])

def ask_university_page(user_id, page):
    start = page * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, len(UNIVERSITIES))
    items = UNIVERSITIES[start:end]
    list_text = format_numbered_list(items, start_from=start + 1)

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

def advance_step(user_id, step_index):
    """Сохраняет прогресс и переход к следующему шагу или завершает анкету."""
    next_idx = step_index + 1
    if next_idx >= len(STEPS):
        set_progress(user_id, next_idx, 0, 2)  # started=2 — анкета завершена
        send_message(user_id, MESSAGES["finished"])
    else:
        set_progress(user_id, next_idx, 0, 1)
        ask_step(user_id, STEPS[next_idx])

def export_to_table(admin_id):
    conn = get_db()
    c = conn.cursor(cursor_factory=RealDictCursor)
    c.execute("SELECT * FROM answers")
    rows = c.fetchall()
    conn.close()
    if not rows:
        send_message(admin_id, MESSAGES["no_data"])
        return
    out = io.StringIO()
    cols = list(rows[0].keys())
    writer = csv.DictWriter(out, fieldnames=cols)
    writer.writeheader()
    for r in rows:
        writer.writerow(dict(r))
    fname = "survey_export.csv"
    with open(fname, "w", encoding="utf-8-sig") as f:
        f.write(out.getvalue())
    out.close()
    try:
        upload = vk_api.VkUpload(vk_session)
        doc = upload.document_message(fname, title="Выгрузка анкет")
        att = f"doc{doc['owner_id']}_{doc['id']}"
        send_message(admin_id, "Вот выгрузка собранных анкет:", attachment=att)
    except Exception as e:
        send_message(admin_id, f"Ошибка при загрузке файла: {e}")
    finally:
        if os.path.exists(fname):
            os.remove(fname)

# ----------------- ОСНОВНАЯ ЛОГИКА -----------------

def handle_message(event):
    user_id = event.user_id
    text = event.text.strip()

    # Команды
    if text.lower() in ["/export", "/выгрузить"]:
        if user_id in ADMIN_IDS:
            export_to_table(user_id)
        else:
            send_message(user_id, MESSAGES["admin_only"])
        return

    if text.lower() == "/restart":
        set_progress(user_id, 0, 0, 0)
        send_message(user_id, "Анкета сброшена. Нажмите «Начать анкету».", kb_start())
        return

    step_index, uni_page, started = get_progress(user_id)

    # Анкета не начата
    if started == 0:
        if text == "Начать анкету":
            set_progress(user_id, 0, 0, 1)
            ask_step(user_id, STEPS[0])
        else:
            send_message(user_id, MESSAGES["welcome"], kb_start())
        return

    # Анкета уже завершена
    if started == 2 or step_index >= len(STEPS):
        send_message(user_id, MESSAGES["already_finished"])
        return

    step_key = STEPS[step_index]

    # --- Выбор вуза ---
    if step_key == "institution":
        if text.lower() in ["далее", ">"]:
            max_page = (len(UNIVERSITIES) - 1) // ITEMS_PER_PAGE
            if uni_page < max_page:
                set_progress(user_id, step_index, uni_page + 1, 1)
                ask_university_page(user_id, uni_page + 1)
            else:
                send_message(user_id, "Это последняя страница.")
                ask_university_page(user_id, uni_page)
            return
        elif text.lower() in ["назад", "<", "←"]:
            if uni_page > 0:
                set_progress(user_id, step_index, uni_page - 1, 1)
                ask_university_page(user_id, uni_page - 1)
            else:
                send_message(user_id, "Это первая страница.")
                ask_university_page(user_id, uni_page)
            return

        if text.isdigit():
            idx = int(text) - 1
            if 0 <= idx < len(UNIVERSITIES):
                save_answer(user_id, "institution", UNIVERSITIES[idx])
                advance_step(user_id, step_index)
                return

        send_message(user_id, "Пожалуйста, введите номер учебного заведения из списка или используйте «далее» / «назад».")
        ask_university_page(user_id, uni_page)
        return

    # --- Контакты ---
    if step_key == "contacts":
        ok, value = validate_contact(text)
        if ok:
            save_answer(user_id, "contacts", value)
            advance_step(user_id, step_index)
        else:
            send_message(user_id, MESSAGES["invalid_contact"])
        return

    # --- Вопросы с вариантами ---
    if step_key in OPTIONS:
        opts = OPTIONS[step_key]
        uses_buttons = all_options_fit(opts)

        if uses_buttons:
            # Режим кнопок
            opts_lower = [o.lower() for o in opts]
            if text.lower() in opts_lower:
                for o in opts:
                    if o.lower() == text.lower():
                        save_answer(user_id, STEP_TO_DB[step_key], o)
                        break
                advance_step(user_id, step_index)
            else:
                send_message(user_id, MESSAGES["invalid_choice"])
            return
        else:
            # Режим нумерованного списка
            if step_key in MULTI_STEPS:
                nums = parse_multi_numbers(text, len(opts))
                if nums is None:
                    send_message(user_id, MESSAGES["invalid_multi"].format(len(opts)))
                    return
                label = "; ".join(opts[n - 1] for n in nums)
                save_answer(user_id, STEP_TO_DB[step_key], label)
            else:
                n = parse_single_number(text, len(opts))
                if n is None:
                    send_message(user_id, MESSAGES["invalid_number"].format(len(opts)))
                    return
                save_answer(user_id, STEP_TO_DB[step_key], opts[n - 1])

            advance_step(user_id, step_index)
            return

    # --- Свободный ввод ---
    if len(text) < 2:
        send_message(user_id, "Пожалуйста, введите более развёрнутый ответ.")
        return
    save_answer(user_id, STEP_TO_DB[step_key], text)
    advance_step(user_id, step_index)

# ----------------- ЗАПУСК -----------------

def main():
    init_db()
    print("Бот запущен...")
    while True:
        try:
            for event in longpoll.listen():
                if event.type == VkEventType.MESSAGE_NEW and event.to_me:
                    handle_message(event)
        except Exception as e:
            print(f"Ошибка в цикле: {e}")
            time.sleep(3)

if __name__ == "__main__":
    main()
