import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.utils import get_random_id
import re
import csv
import io
import os

# ================= НАСТРОЙКИ (ЗАПОЛНИТЬ ОБЯЗАТЕЛЬНО) =================
TOKEN = "ВАШ_API_ТОКЕН"  # Токен сообщества
ADMIN_IDS = {123456789, 987654321}  # ID администраторов (через запятую в фигурных скобках)
GROUP_ID = 12345678  # ID группы (число, без club)
# =====================================================================

# Инициализация VK API
vk_session = vk_api.VkApi(token=TOKEN)
vk = vk_session.get_api()
longpoll = VkLongPoll(vk_session)

# Хранилище данных пользователей (в памяти, при перезапуске сбросится)
# Для продакшена лучше использовать базу данных (SQLite/PostgreSQL)
user_data = {}

# ----------------- СПИСКИ И КОНФИГУРАЦИЯ -----------------

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

# Шаги анкеты (порядок важен)
STEPS = [
    "fio", "birth_date", "phone", "email", "education_level", "institution", "specialty", "group",
    "course_type", "employment_status", "work_experience", "post_graduation_plans", 
    "help_from_work_russia", "special_status", "career_plans", "admin_help"
]

# Тексты вопросов (Локализация)
QUESTIONS = {
    "fio": "Пожалуйста, укажите ваши ФИО полностью:",
    "birth_date": "Укажите дату рождения (ДД.ММ.ГГГГ):",
    "phone": "Введите номер телефона (например, +79991234567):",
    "email": "Введите адрес электронной почты:",
    "education_level": "Выберите уровень образования:",
    "institution": "Выберите учебное заведение из списка:",
    "specialty": "Укажите вашу специальность/направление подготовки:",
    "group": "Укажите номер вашей учебной группы:",
    "course_type": "Форма обучения:",
    "employment_status": "Ваш текущий статус занятости:",
    "work_experience": "Есть ли у вас опыт работы по специальности или смежной с ней?",
    "post_graduation_plans": "Какие планы после окончания обучения?",
    "help_from_work_russia": "Как вы в целом оцениваете результаты своих профориентационных практик/работы с работодателями по специальности обучения?",
    "special_status": "Есть ли у вас особый статус или жизненные обстоятельства?",
    "career_plans": "Планируете ли вы оставаться в регионе после окончания учебы?",
    "admin_help": "Команда для админов: /export (выгрузка данных)"
}

# Варианты ответов (кнопки)
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

# Сообщения об ошибках (Локализация)
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

def init_user(user_id):
    user_data[user_id] = {
        "step_index": 0,
        "answers": {},
        "university_page": 0
    }

def send_message(user_id, message, keyboard=None):
    """Отправка сообщения с опциональной клавиатурой"""
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
    """Генерация клавиатуры в зависимости от шага"""
    
    # Клавиатура для выбора ВУЗа (постраничная)
    if step_key == "institution":
        start = page * ITEMS_PER_PAGE
        end = start + ITEMS_PER_PAGE
        items = UNIVERSITIES[start:end]
        
        keyboard = {"one_time": False, "buttons": []}
        
        # Кнопки с ВУЗами
        for i in range(0, len(items), 2):
            row = []
            row.append({"action": {"type": "text", "label": items[i]}, "color": "primary"})
            if i + 1 < len(items):
                row.append({"action": {"type": "text", "label": items[i+1]}, "color": "primary"})
            keyboard["buttons"].append(row)
        
        # Навигация
        nav_row = []
        if page > 0:
            nav_row.append({"action": {"type": "text", "label": "← Назад"}, "color": "secondary"})
        if end < len(UNIVERSITIES):
            nav_row.append({"action": {"type": "text", "label": "Далее →"}, "color": "secondary"})
        
        if nav_row:
            keyboard["buttons"].append(nav_row)
            
        return keyboard

    # Клавиатура для обычных вариантов выбора
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
    """Валидация телефона и почты"""
    text = text.strip()
    
    if field_type == 'phone':
        # Регулярка для +7, 8, с пробелами и скобками
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
    
    return False, None

def export_to_table(admin_id):
    """Выгрузка данных в CSV"""
    if not user_data:
        send_message(admin_id, MESSAGES["no_data"])
        return

    output = io.StringIO()
    # Берем ключи из первого пользователя для заголовков
    first_user = next(iter(user_data.values()))
    fieldnames = ["user_id"] + list(first_user["answers"].keys())
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for uid, data in user_data.items():
        row = {"user_id": uid}
        row.update(data["answers"])
        writer.writerow(row)
    
    csv_content = output.getvalue()
    output.close()

    filename = "survey_export.csv"
    with open(filename, "w", encoding="utf-8-sig") as f:
        f.write(csv_content)

    # Загрузка документа в ВК
    upload = vk_api.VkUpload(vk_session)
    try:
        doc = upload.document_message(filename, title="Выгрузка анкет")
        attachment = f"doc{doc['owner_id']}_{doc['id']}"
        send_message(admin_id, "Вот выгрузка собранных анкет:", attachment=attachment)
    except Exception as e:
        send_message(admin_id, f"Ошибка при загрузке файла: {e}")
    finally:
        if os.path.exists(filename):
            os.remove(filename)

# ----------------- ОСНОВНАЯ ЛОГИКА -----------------

def handle_message(event):
    user_id = event.user_id
    text = event.text.strip()

    # Проверка на команду админа
    if text.lower() in ["/export", "/выгрузить"]:
        if user_id in ADMIN_IDS:
            export_to_table(user_id)
        else:
            send_message(user_id, MESSAGES["admin_only"])
        return

    # Инициализация пользователя, если нет в базе
    if user_id not in user_data:
        init_user(user_id)
        # Сразу начинаем опрос
        current_step = STEPS
        keyboard = get_keyboard(current_step)
        send_message(user_id, QUESTIONS[current_step], keyboard)
        return

    data = user_data[user_id]
    current_step_key = STEPS[data["step_index"]]

    # 1. Логика выбора ВУЗа (с навигацией)
    if current_step_key == "institution":
        if text == "← Назад":
            if data["university_page"] > 0:
                data["university_page"] -= 1
                keyboard = get_keyboard(current_step_key, data["university_page"])
                send_message(user_id, QUESTIONS[current_step_key], keyboard)
            else:
                send_message(user_id, "Это первая страница списка. Выберите учебное заведение.")
            return
        elif text == "Далее →":
            max_page = (len(UNIVERSITIES) - 1) // ITEMS_PER_PAGE
            if data["university_page"] < max_page:
                data["university_page"] += 1
                keyboard = get_keyboard(current_step_key, data["university_page"])
                send_message(user_id, QUESTIONS[current_step_key], keyboard)
            else:
                send_message(user_id, "Это последняя страница списка. Выберите учебное заведение.")
            return
        elif text in UNIVERSITIES:
            data["answers"][current_step_key] = text
            data["step_index"] += 1
            # Переход к следующему шагу
            if data["step_index"] >= len(STEPS):
                send_message(user_id, "Спасибо! Анкета успешно заполнена.")
                # Здесь можно добавить логику сохранения в БД навсегда
            else:
                next_step = STEPS[data["step_index"]]
                keyboard = get_keyboard(next_step)
                send_message(user_id, QUESTIONS[next_step], keyboard)
            return
        else:
            send_message(user_id, "Пожалуйста, выберите учебное заведение из кнопок или используйте «← Назад» / «Далее →».")
            return

    # 2. Логика валидации контактов (Телефон и Почта)
    if current_step_key in ["phone", "email"]:
        field_type = "phone" if current_step_key == "phone" else "email"
        is_valid, value = validate_contact(text, field_type)
        
        if is_valid:
            data["answers"][current_step_key] = value
            data["step_index"] += 1
            
            if data["step_index"] >= len(STEPS):
                send_message(user_id, "Спасибо! Анкета успешно заполнена.")
            else:
                next_step = STEPS[data["step_index"]]
                keyboard = get_keyboard(next_step)
                send_message(user_id, QUESTIONS[next_step], keyboard)
            return
        else:
            send_message(user_id, MESSAGES["invalid_contact"])
            return

    # 3. Логика выбора из кнопок (обычные варианты)
    if current_step_key in OPTIONS:
        options_lower = [opt.lower() for opt in OPTIONS[current_step_key]]
        if text.lower() not in options_lower:
            send_message(user_id, MESSAGES["invalid_choice"])
            return
        
        # Находим оригинальный вариант с правильным регистром
        for opt in OPTIONS[current_step_key]:
            if opt.lower() == text.lower():
                data["answers"][current_step_key] = opt
                break
        
        data["step_index"] += 1
        if data["step_index"] >= len(STEPS):
            send_message(user_id, "Спасибо! Анкета успешно заполнена.")
        else:
            next_step = STEPS[data["step_index"]]
            keyboard = get_keyboard(next_step)
            send_message(user_id, QUESTIONS[next_step], keyboard)
        return

    # 4. Свободный ввод (ФИО, Дата, Специальность, Группа и т.д.)
    # Для даты можно добавить простую проверку формата, но пока принимаем любой текст
    data["answers"][current_step_key] = text
    data["step_index"] += 1
    
    if data["step_index"] >= len(STEPS):
        send_message(user_id, "Спасибо! Анкета успешно заполнена.")
    else:
        next_step = STEPS[data["step_index"]]
        keyboard = get_keyboard(next_step)
        send_message(user_id, QUESTIONS[next_step], keyboard)

# ----------------- ЗАПУСК -----------------

def main():
    print("Бот запущен...")
    try:
        for event in longpoll.listen():
            if event.type == VkEventType.MESSAGE_NEW and event.to_me:
                handle_message(event)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        # В продакшене здесь нужна перезапуск логики или запись в лог

if __name__ == "__main__":
    main()
