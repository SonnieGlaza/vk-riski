import os
import re
import csv
import io
from datetime import datetime

import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.utils import get_random_id
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base

# ================= НАСТРОЙКИ ИЗ ENV =================
VK_TOKEN = os.getenv("VK_TOKEN")
ADMIN_IDS = set(map(int, os.getenv("ADMIN_IDS", "").split(","))) if os.getenv("ADMIN_IDS") else set()
GROUP_ID = int(os.getenv("GROUP_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")

if not VK_TOKEN or not DATABASE_URL:
    raise ValueError("Не заданы переменные окружения VK_TOKEN или DATABASE_URL")
# =====================================================

# ----------------- НАСТРОЙКА БАЗЫ ДАННЫХ -----------------
Base = declarative_base()

class SurveyResponse(Base):
    __tablename__ = "survey_responses"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, index=True)
    fio = Column(String(255))
    birth_date = Column(String(50))
    phone = Column(String(50))
    email = Column(String(255))
    education_level = Column(String(100))
    institution = Column(Text)
    specialty = Column(Text)
    group_name = Column(String(100))  # group - зарезервированное слово в SQL
    course_type = Column(String(100))
    employment_status = Column(String(100))
    work_experience = Column(String(100))
    post_graduation_plans = Column(String(255))
    help_from_work_russia = Column(String(100))
    special_status = Column(String(255))
    career_plans = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

# Создаем таблицы при старте (для Railway это безопасно: если таблицы есть, ничего не произойдет)
Base.metadata.create_all(bind=engine)

# ----------------- ДАННЫЕ И КОНФИГУРАЦИЯ -----------------
UNIVERSITIES = [
    "БПОУ УР «Воткинский промышленный техникум»", "БПОУ УР «Воткинский музыкально-педагогический колледж имени П.И. Чайковского»",
    "БПОУ УР «Воткинский машиностроительный техникум имени В.Г.Садовникова»", "АПОУ УР «Глазовский аграрно-промышленный техникум»",
    "БПОУ УР «Глазовский технический колледж»", "БПОУ «Дебесский политехникум»", "БПОУР «Игринский политехнический техникум»",
    "БПОУ УР «Ижевский торгово-экономический техникум»", "БПОУ УР «Ижевский монтажный техникум»", "БПОУ «Ижевский агростроительный техникум»",
    "ЧПОО «Нефтяной техникум»", "БПОУ УР «Ижевский политехнический колледж»", "БПОУ УР «Ижевский промышленно-экономический колледж»",
    "БПОУ УР «Ижевский машиностроительный техникум им. С.Н. Борина»", "БПОУ УР «Радиомеханический техникум имени В.А. Шутова»",
    "АПОУ УР «Экономико-технологический колледж»", "БПОУ УР «Асановский аграрно-технический техникум»", "АПОУ УР «Топливно-энергетический колледж»",
    "БПОУ УР «Ижевский техникум индустрии питания»", "КПОУ УР «Удмуртский республиканский колледж культуры»",
    "АНПОО «Международный Восточно-Европейский колледж»", "АПОУ УР «Техникум радиоэлектроники и информационных технологий им. А.В. Воскресенского»",
    "АПОУ УР «Республиканский медицинский колледж имени Героя Советского Союза Ф.А. Пушиной Министерства здравоохранения Удмуртской Республики»",
    "ПОЧУ «Ижевский техникум экономики, управления и права Удмуртпотребсоюза»", "АНПОО СПО «Ижевский финансово-юридический колледж»",
    "БПОУ УР «Удмуртский республиканский социально-педагогический колледж»", "АПОУ УР «Строительный техникум»",
    "ФГБОУ ВО «Ижевская государственная медицинская академия»", "КПОУ УР «Республиканский музыкальный колледж»",
    "БПОУ УР «Ижевский промышленно-экономический колледж» в г. Можга", "БПОУ УР «Можгинский педагогический колледж имени Т.К. Борисова»",
    "БПОУ УР «Можгинский агропромышленный колледж»", "БПОУ УР «Сарапульский политехнический техникум»",
    "БПОУ УР «Сарапульский многопрофильный колледж»", "БПОУ УР «Сарапульский колледж социально-педагогических технологий и сервиса»",
    "БПОУ УР «Сюмсинский техникум лесного и сельского хозяйства»", "БПОУ УР «Увинский профессиональный колледж»",
    "БПОУ УР «Ярский политехникум»", "ФГБОУ ВО «Приволжский государственный университет путей сообщения»",
    "БПОУ УР «Ижевский индустриальный техникум имени Евгения Фёдоровича Драгунова»", "БПОУ УР «Глазовский политехнический колледж»",
    "Министерство юстиции Российской Федерации", "ФГБОУ ВО «Удмуртский государственный университет»", "ФГБОУ ВО «Удмуртский государственный аграрный университет»", 
    "ФГБОУ ВО «Ижевский государственный технический университет имени М.Т. Калашникова»", "БПОУ УР «Ижевский автотранспортный техникум»", 
    "ФГБОУ ВО «Глазовский государственный инженерно-педагогический университет имени В. Г. Короленко»", "Сарапульский техникум машиностроения и информационных технологий"
]

ITEMS_PER_PAGE = 10

STEPS = [
    "fio", "birth_date", "phone", "email", "education_level", "institution", "specialty", "group_name",
    "course_type", "employment_status", "work_experience", "post_graduation_plans", 
    "help_from_work_russia", "special_status", "career_plans"
]

QUESTIONS = {
    "fio": "Пожалуйста, укажите ваши ФИО полностью:",
    "birth_date": "Укажите дату рождения (ДД.ММ.ГГГГ):",
    "phone": "Введите номер телефона (например, +79991234567):",
    "email": "Введите адрес электронной почты:",
    "education_level": "Выберите уровень образования:",
    "institution": "Выберите учебное заведение из списка:",
    "specialty": "Укажите вашу специальность/направление подготовки:",
    "group_name": "Укажите номер вашей учебной группы:",
    "course_type": "Форма обучения:",
    "employment_status": "Ваш текущий статус занятости:",
    "work_experience": "Есть ли у вас опыт работы по специальности или смежной с ней?",
    "post_graduation_plans": "Какие планы после окончания обучения?",
    "help_from_work_russia": "Как вы в целом оцениваете результаты своих профориентационных практик/работы с работодателями по специальности обучения?",
    "special_status": "Есть ли у вас особый статус или жизненные обстоятельства?",
    "career_plans": "Планируете ли вы оставаться в регионе после окончания учебы?"
}

OPTIONS = {
    "education_level": ["СПО (Среднее профессиональное)", "ВО (Высшее образование)", "Другое"],
    "course_type": ["Очная", "Очно-заочная", "Заочная", "Дистанционная"],
    "employment_status": ["Работаю по специальности", "Работаю не по специальности", "Не работаю", "Нахожусь в декрете", "Другое"],
    "work_experience": ["Да, есть опыт", "Нет, опыта нет", "Есть стажировки"],
    "post_graduation_plans": ["Планирую работать по специальности", "Планирую продолжить обучение", "Планирую открыть свое дело", "Другое"],
    "help_from_work_russia": ["Отлично", "Хорошо", "Удовлетворительно", "Плохо", "Не участвовал"],
    "special_status": ["Инвалидность", "Сирота", "Многодетная семья", "Нет особых статусов"],
    "career_plans": ["Да, планирую остаться", "Нет, планирую уехать", "Пока не решил(а)"]
}

MESSAGES = {
    "invalid_contact": "Не удалось распознать контакт. Пожалуйста, введите:\n"
                        "• Номер телефона в формате +79991234567 или 89991234567\n"
                        "или\n"
                        "• Адрес электронной почты в формате example@mail.ru",
    "invalid_choice": "Пожалуйста, выберите вариант из кнопок ниже, чтобы избежать ошибок.",
    "no_data": "Пока нет собранных анкет для выгрузки.",
    "admin_only": "Эта команда доступна только администраторам."
}

# ----------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ -----------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_user_state(user_id, db):
    # В памяти храним только текущее состояние шага и страницу ВУЗа
    # Данные анкеты сохраняются в БД только по завершении
    if user_id not in user_states:
        user_states[user_id] = {"step_index": 0, "university_page": 0}

def send_message(user_id, message, keyboard=None):
    try:
        vk.messages.send(
            user_id=user_id,
            message=message,
            random_id=get_random_id(),
            keyboard=keyboard
        )
    except Exception as e:
        print(f"Ошибка отправки сообщения: {e}")

def get_keyboard(step_key, page=0):
    if step_key == "institution":
        start = page * ITEMS_PER_PAGE
        end = start + ITEMS_PER_PAGE
        items = UNIVERSITIES[start:end]
        
        keyboard = {"one_time": False, "buttons": []}
        for i in range(0, len(items), 2):
            row = []
            row.append({"action": {"type": "text", "label": items[i]}, "color": "primary"})
            if i + 1 < len(items):
                row.append({"action": {"type": "text", "label": items[i+1]}, "color": "primary"})
            keyboard["buttons"].append(row)
        
        nav_row = []
        if page > 0:
            nav_row.append({"action": {"type": "text", "label": "← Назад"}, "color": "secondary"})
        if end < len(UNIVERSITIES):
            nav_row.append({"action": {"type": "text", "label": "Далее →"}, "color": "secondary"})
        
        if nav_row:
            keyboard["buttons"].append(nav_row)
        return keyboard

    if step_key in OPTIONS:
        keyboard = {"one_time": False, "buttons": []}
        options = OPTIONS[step_key]
        for i in range(0, len(options), 2):
            row = []
            row.append({"action": {"type": "text", "label": options[i]}, "color": "primary"})
            if i + 1 < len(options):
                row.append({"action": {"type": "text", "label": options[i+1]}, "color": "primary"})
            keyboard["buttons"].append(row)
        return keyboard
    
    return None

def validate_contact(text, field_type):
    text = text.strip()
    if field_type == 'phone':
        pattern = r'^\+??[\s\-]?$?\d{3}$?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}\$'
        if re.match(pattern, text):
            digits = re.sub(r'\D', '', text)
            if len(digits) == 11 and digits.startswith('8'):
                digits = '7' + digits[1:]
            elif len(digits) == 10:
                digits = '7' + digits
            return True, '+7' + digits[-10:]
        return False, None
    elif field_type == 'email':
        pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\$'
        if re.match(pattern, text):
            return True, text.lower()
    return False, None

def save_response_to_db(user_id, answers):
    db = SessionLocal()
    try:
        # Проверяем, нет ли уже ответа от этого пользователя (опционально)
        existing = db.query(SurveyResponse).filter_by(user_id=user_id).first()
        if existing:
            # Обновляем поля
            for key, value in answers.items():
                setattr(existing, key, value)
            existing.created_at = datetime.utcnow()
        else:
            # Создаем новую запись
            new_entry = SurveyResponse(user_id=user_id, **answers)
            db.add(new_entry)
        db.commit()
        return True
    except Exception as e:
        print(f"Ошибка сохранения в БД: {e}")
        db.rollback()
        return False
    finally:
        db.close()

def export_to_csv():
    db = SessionLocal()
    try:
        rows = db.query(SurveyResponse).all()
        if not rows:
            return None
        
        output = io.StringIO()
        fieldnames = [c.name for c in SurveyResponse.__table__.columns]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in rows:
            writer.writerow({c.name: getattr(row, c.name) for c in SurveyResponse.__table__.columns})
        
        return output.getvalue()
    except Exception as e:
        print(f"Ошибка экспорта: {e}")
        return None
    finally:
        db.close()

# Глобальное состояние в памяти (сбрасывается при рестарте, но данные в БД остаются)
user_states = {}

# ----------------- ИНИЦИАЛИЗАЦИЯ VK API -----------------
vk_session = vk_api.VkApi(token=VK_TOKEN)
vk = vk_session.get_api()
longpoll = VkLongPoll(vk_session)

# ----------------- ОСНОВНОЙ ЦИКЛ -----------------
print("Бот запущен и ожидает сообщения...")

for event in longpoll.listen():
    if event.type == VkEventType.MESSAGE_NEW and event.to_me:
        user_id = event.user_id
        text = event.text.strip()
        
        # Инициализация состояния пользователя
        init_user_state(user_id, None)
        state = user_states[user_id]
        
        # Обработка команды админа
        if text == "/export":
            if user_id not in ADMIN_IDS:
                send_message(user_id, MESSAGES["admin_only"])
                continue
            
            csv_data = export_to_csv()
            if not csv_data:
                send_message(user_id, MESSAGES["no_data"])
                continue
            
            filename = "survey_export.csv"
            with open(filename, "w", encoding="utf-8-sig") as f:
                f.write(csv_data)
            
            upload = vk_api.VkUpload(vk_session)
            doc = upload.document_message(file_path=filename, title="Выгрузка анкет", tags=[])
            
            owner_id = doc["owner_id"]
            media_id = doc["id"]
            
            vk.messages.send(
                user_id=user_id,
                message="Вот выгрузка собранных анкет:",
                random_id=get_random_id(),
                attachment=f"doc{owner_id}_{media_id}"
            )
            os.remove(filename)
            continue
        
        # Логика анкеты
        current_step_key = STEPS[state["step_index"]]
        
        # Обработка навигации по ВУЗам
        if current_step_key == "institution":
            if text == "← Назад":
                if state["university_page"] > 0:
                    state["university_page"] -= 1
                send_message(user_id, QUESTIONS["institution"], get_keyboard("institution", state["university_page"]))
                continue
            elif text == "Далее →":
                if state["university_page"] * ITEMS_PER_PAGE < len(UNIVERSITIES):
                    state["university_page"] += 1
                send_message(user_id, QUESTIONS["institution"], get_keyboard("institution", state["university_page"]))
                continue
        
        # Валидация и сохранение ответа
        answer_value = None
        
        if current_step_key in ["phone", "email"]:
            is_valid, normalized = validate_contact(text, current_step_key)
            if not is_valid:
                send_message(user_id, MESSAGES["invalid_contact"], get_keyboard(current_step_key))
                continue
            answer_value = normalized
        elif current_step_key == "institution":
            if text not in UNIVERSITIES:
                send_message(user_id, MESSAGES["invalid_choice"], get_keyboard("institution", state["university_page"]))
                continue
            answer_value = text
        elif current_step_key in OPTIONS:
            if text not in OPTIONS[current_step_key]:
                send_message(user_id, MESSAGES["invalid_choice"], get_keyboard(current_step_key))
                continue
            answer_value = text
        else:
            # Текстовые поля (ФИО, специальность и т.д.)
            if not text:
                send_message(user_id, "Пожалуйста, введите значение.")
                continue
            answer_value = text
        
        # Сохраняем ответ во временное состояние
        if "temp_answers" not in state:
            state["temp_answers"] = {}
        state["temp_answers"][current_step_key] = answer_value
        
        # Переход к следующему шагу
        next_index = state["step_index"] + 1
        if next_index >= len(STEPS):
            # Анкета завершена
            success = save_response_to_db(user_id, state["temp_answers"])
            if success:
                send_message(user_id, "Спасибо! Ваша анкета успешно сохранена в базе данных.")
            else:
                send_message(user_id, "Произошла ошибка при сохранении. Попробуйте позже.")
            
            # Сброс состояния
            del user_states[user_id]
        else:
            state["step_index"] = next_index
            next_step_key = STEPS[state["step_index"]]
            keyboard = get_keyboard(next_step_key, state.get("university_page", 0))
            send_message(user_id, QUESTIONS[next_step_key], keyboard)
