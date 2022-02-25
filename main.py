import telebot
import sqlite3
import time
import datetime
import json
import pickle
import logging
from logging.handlers import TimedRotatingFileHandler
import traceback
import os
import sys
import configparser
import threading
import schedule
import docxtpl


# Глобальные переменные
# идентификатор текущего открытого инцидента, -1 если нет открытых
ki_current_id = -1

# идентификатор предыдущего открытого инцидента, -1 если ранее не было инцидентов
ki_current_status = -1

# набор id чатов, где зарегистрирован бот
bot_chat_list = []

# экземпляр bot
bot = 0


# метод для формирования имени файла для ротационного логгера
def get_filename(filename):
    # Получаем директорию, где расположены логи
    log_directory = os.path.split(filename)[0]

    # suffix - это расширение (с точкой) файла.
    # У нас - %Y%m%d. Например .20181231.
    # Точка нам не нужна, т.к. файл будет называться suffix.log (20181231.log)
    date = os.path.splitext(filename)[1][1:]

    # Сформировали имя нового лог-файла
    filename = os.path.join(log_directory, date)

    if not os.path.exists('{}.log'.format(filename)):
        return '{}.log'.format(filename)

    # Найдём минимальный индекс файла на текущий момент.
    index = 0
    f = '{}.{}.log'.format(filename, index)
    while os.path.exists(f):
        index += 1
        f = '{}.{}.log'.format(filename, index)
    return f


# прочитаем конфигурационный файл
config = configparser.ConfigParser()
try:
    print("Загрузка конфигурационного файла..")
    config.read('config.ini')
except OSError as error:
    sys.exit("Не найден конфигурационный файл! не могу стартовать, нет API-KEY!")

logger = logging.getLogger("my_log")
print("Установка errorlevel=" + str(config['MAIN']['errorlevel']))

match config['MAIN']['errorlevel']:
    case 'DEBUG':
        logger.setLevel(logging.DEBUG)
    case 'INFO':
        logger.setLevel(logging.INFO)
    case 'WARNING':
        logger.setLevel(logging.WARNING)
    case 'ERROR':
        logger.setLevel(logging.ERROR)
    case _:
        logger.setLevel(logging.DEBUG)

# https://makesomecode.me/2019/03/python-log-rotation/
# ротационный файловый логгер
rotation_logging_handler = TimedRotatingFileHandler('./logs/log.log', when='d', interval=1, backupCount=5)
rotation_logging_handler.suffix = '%Y%m%d'
rotation_logging_handler.namer = get_filename
logger.addHandler(rotation_logging_handler)

# задаем базовый формат сообщения в логе
# https://habr.com/ru/sandbox/150814/
basic_formatter = logging.Formatter('%(asctime)s : [%(levelname)s] : %(message)s')
rotation_logging_handler.setFormatter(basic_formatter)


# метод для вывода детальной информации об ошибке Python в лог
# https://habr.com/ru/sandbox/150814/
def error_log(line_no):
    # кастомный форматтер
    ef = logging.Formatter('%(asctime)s : [%(levelname)s][LINE ' + line_no + '] : %(message)s')
    rotation_logging_handler.setFormatter(ef)
    logger.addHandler(rotation_logging_handler)
    # пишем сообщение error
    logger.error(traceback.format_exc())
    # возвращаем базовый формат сообщений
    rotation_logging_handler.setFormatter(basic_formatter)
    logger.addHandler(rotation_logging_handler)
    return


# инициализация БД
def init_db():
    sqlite_connection = 0
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        cursor = sqlite_connection.cursor()

        cursor.execute("DROP TABLE bot_chat_log_ext")
        sqlite_connection.commit()
        cursor.execute("DROP TABLE incident_comment_data")
        sqlite_connection.commit()

        cursor.execute(
            "CREATE TABLE IF NOT EXISTS bot_chat_log_ext (id INTEGER PRIMARY KEY, "
            "open_time TEXT NOT NULL, "
            "initiator TEXT NOT NULL, "
            "ki_open_info TEXT NOT NULL,"
            "close_time TEXT,"
            "close_manager TEXT,"
            "ki_close_info TEXT,"
            "status INTEGER NOT NULL,"
            "system INTEGER DEFAULT 0 NOT NULL );")
        sqlite_connection.commit()

        cursor.execute(
            "CREATE TABLE IF NOT EXISTS incident_comment_data (id INTEGER PRIMARY KEY, "
            "fk_id INTEGER NOT NULL, "
            "comment_time TEXT NOT NULL, "
            "commentator TEXT NOT NULL, "
            "comment TEXT NOT NULL,"
            "data BLOB);")
        sqlite_connection.commit()
    except sqlite3.Error as error:
        logger.error("Ошибка при подключении к sqlite", error)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            print("initdb: соединение с SQLite закрыто")


# функция записи в БД
def write_to_db(s, datatuple):
    sqlite_connection = 0
    logger.info("-> write_to_db")
    logger.debug("  " + str(datatuple))
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        cursor = sqlite_connection.cursor()
        cursor.execute(s, datatuple)
        sqlite_connection.commit()
    except sqlite3.Error as error:
        logger.error("   Ошибка при подключении к sqlite" + str(error))
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            logger.debug("   Cоединение с SQLite закрыто!")
    logger.info("<- write_to_db")


# метод поиска и записи в БД #hashword
# параметр from_where:
# 1 - ицнцидент
# 2 - комменатрий к инциденту
# 3 - issue
# 4 - комментарий к issue
def check_and_save_hashword(msg, from_where, index):
    where_id = (from_where, index)
   # каждое ключевое слово записываем в отдельную строку
    for m in msg:
        if str(m).startswith("#"):
            # нашли ключевое слово
            datatuple = (str(m), str(where_id))
            write_to_db("INSERT INTO hashword_tbl(hashword, where_id) VALUES(?,?)",
                        datatuple)


def fetch_data_with_hashword(hashword):
    # 1. Сначала найдем hashword в таблице
    res = ()
    logger.info("-> search_for_a_hashword: " + hashword)
    sqlite_connection = 0
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        # получим все упоминания hashword
        cursor = sqlite_connection.cursor()
        cursor.execute("select where_id from hashword_tbl WHERE "
                       "hashword='" + str(hashword) + "'")
        res = cursor.fetchall()
    except sqlite3.Error as error:
        logger.error(error)
        frame = traceback.extract_tb(sys.exc_info()[2])
        line_no = str(frame[0]).split()[4]
        error_log(line_no)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            logger.debug("Соединение с SQLite закрыто")

    if len(res) == 0:
        return None

    for datatuple in res:
        a = eval(datatuple[0])
        # первое волшебное число это тип таблицы для поиска, второй - индекс в таблице поиска
        s = ""
        match a[0]:
            case 1:
                pass
            case 2:
                s = "select comment, commentator, comment_time, fk_id from incident_comment_data where id=" + str(a[1])
                pass
            case 3:
                pass
            case 4:
                pass
            case _:
                pass

        try:
            sqlite_connection = sqlite3.connect('sqlite_python.db')
            # получим все упоминания hashword
            cursor = sqlite_connection.cursor()
            cursor.execute(s)
            res = cursor.fetchall()
        except sqlite3.Error as error:
            logger.error(error)
            frame = traceback.extract_tb(sys.exc_info()[2])
            line_no = str(frame[0]).split()[4]
            error_log(line_no)
        finally:
            if sqlite_connection:
                sqlite_connection.close()
                logger.debug("Соединение с SQLite закрыто")

    return res[0]

#############################################################################
## Методы обслуживания технических отчетов
#############################################################################
# открыть новый отчет, добавить #хештег и тему отчета
def open_new_issue_command():
    pass


# добавить комменатрий к отчету
def add_issue_comment_command():
    pass


# добавить комменатрий с картинкой к отчету
def add_img_issue_comment_command():
    pass


# финализировать технический отчет, добавить заключение (conclusion)
def close_issue_command():
    pass


# добавить информацию к секции вывода conclusion
def add_conclusion_issue_command():
    pass


# найти отчет, материалы которого содержат искомый #hashtag
def search_issue_command():
    pass


# записать технический отчет в Word
def save_issue_to_docx():
    pass


# записать технический отчет в Excel
def save_issue_to_xlsx():
    pass


#############################################################################
## Методы обслуживания инцидентов
#############################################################################

# метод формирует краткий отчет по инцидентам для чата
def get_incident_report(curr=-1):
    logger.info("-> get_report")
    # строка с отчетом
    report = ""

    sqlite_connection = 0
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        cursor = sqlite_connection.cursor()
        # есть незакрытый инцидент
        if curr == -1:
            cursor.execute("SELECT * FROM bot_chat_log_ext")
        else:
            cursor.execute("SELECT * FROM bot_chat_log_ext WHERE id=?", [curr])
        record = cursor.fetchall()

        if curr == -1:
            report = "Отчет по инцидентам:\n==========================\n"
            if len(record) == 0:
                report += "Инциденты отсутствуют"
            else:
                for s in record:
                    time_start = datetime.datetime.strptime(s[1], "%Y-%m-%d %H:%M:%S")
                    report += "№" + str(s[0]) + " Открыт: " + str(s[1]) + " " + str(s[3])
                    if int(s[7]) == 0:
                        result = "\n№" + str(s[0]) + " Не закрыт!\n"
                    else:
                        time_end = datetime.datetime.strptime(s[4], "%Y-%m-%d %H:%M:%S")
                        diff = time_end - time_start
                        result = "\n№" + str(s[0]) + " Закрыт: " + str(s[4]) + " " + str(s[6]) + "\n"
                        result += "\nДлительность инцидента: " + str(diff)
                    report += result + "\n==========================\n"
                    logger.debug("\n" + report)
        else:
            if len(record) > 0:
                time_start = datetime.datetime.strptime(record[0][1], "%Y-%m-%d %H:%M:%S")
                time_end = datetime.datetime.strptime(record[0][4], "%Y-%m-%d %H:%M:%S")
                diff = time_end - time_start
                report = "\nДлительность инцидента: " + str(diff)
            else:
                report = "\nДлительность инцидента не определена!"
            logger.debug("\n" + report)
        cursor.close()
    except sqlite3.Error as error:
        logger.error("Ошибка при подключении к sqlite: " + str(error))
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            logger.debug("Соединение с SQLite закрыто")
    logger.info("<- get_report")
    return report


# метод выгружает номер и статус инцидента
def get_incident_status():
    global ki_current_id
    global ki_current_status
    global bot_chat_list
    logger.info("-> get_incident_status")
    with open('app.json', 'r') as file_object:
        data = json.load(file_object)
        ki_current_id = data[0]
        ki_current_status = data[1]
    logger.debug("Номер текущего инцидента: " + str(ki_current_id))
    logger.debug("Статус текущего инцидента: " + str(ki_current_status))
    try:
        with open('chats.json', 'r') as file_object:
            data = json.load(file_object)
            bot_chat_list = data
        logger.debug("   Выгружен список id чатов: " + str(bot_chat_list))
    except OSError:
        logger.warning("   При первом запуске бота эта ошибка возможна. "
                     "Нужно зарегистрировать бота командой /register в каждом чате")
        frame = traceback.extract_tb(sys.exc_info()[2])
        line_no = str(frame[0]).split()[4]
        error_log(line_no)
        pass
    logger.info("<- get_incident_status")


# метод сохраняет статус инцидента (номер, статус)
def store_incident_status():
    global ki_current_id
    global ki_current_status
    global bot_chat_list

    logger.info("-> store_incident_status")

    dataduple = [ki_current_id, ki_current_status]
    filename = 'app.json'
    with open(filename, 'w') as file_object:
        json.dump(dataduple, file_object)

    logger.debug("   Пишем статусы: " + str(filename) + " " + str(dataduple))

    filenamechats = 'chats.json'
    with open(filenamechats, 'w') as file_object:
        json.dump(bot_chat_list, file_object)

    logger.debug("   Пишем статусы: " + str(filenamechats) + " " + str(bot_chat_list))
    logger.info("<- store_incident_status")


# записать краткий отчет по инцидентам в csv
def save_incident_report_to_csv(param=0, data=0):
    logger.info("-> save_report_to_csv")
    sqlite_connection = 0
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        cursor = sqlite_connection.cursor()
        # есть незакрытый инцидент
        if param == 0:
            cursor.execute("SELECT * FROM bot_chat_log_ext")
        elif param == 1:
            cursor.execute("SELECT * FROM bot_chat_log_ext WHERE system=?", [data])
        record = cursor.fetchall()

        with open("output.csv", 'w') as file_object:  # open the file in write mode
            file_object.writelines("№;Дата открытия;Инициатор;Описание;Дата закрытия;"
                                   "Кто закрыл;Комментарий при закрытии;Статус;Система(0 = ПЦЛ, 1 = УВР)\n")
            for r in record:
                s = str(r[0]) + ";" + str(r[1]) + ";" + str(r[2]) + ";" + str(r[3]) + ";"\
                      + str(r[4]) + ";" + str(r[5]) + ";" + str(r[6]) + ";" + str(r[7]) + ";"\
                      + str(r[8]) + "\n"
                file_object.writelines(s)
            logger.debug("   " + s)
        cursor.close()
    except sqlite3.Error as error:
        logger.error("Ошибка при подключении к sqlite: " + str(error))
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            logger.debug("   Соединение с SQLite закрыто")
    logger.info("<- save_report_to_csv")
    return


# записать краткий отчет по инцидентам в Word
def save_short_incident_report_to_docx():
    pass


# записать полный отчет по инцидентам в Word
def save_full_incident_report_to_docx():
    pass


# метод добавления комментария к инциденту
def add_incident_comment(msg, chat_id, username, photo=0):
    logger.info("-> add_comment")
    global ki_current_id
    global ki_current_status

    get_incident_status()

    num = -1
    # без фотографии нельзя добавлять пустые комментарии, с фотографией - можно
    if photo == 0:
        if len(msg) == 1:
            if ki_current_status == -1 or ki_current_status == 1:
                bot.send_message(chat_id, "Нет открытых инцидентов, введите /add [номер_инцидента] [комментарий] "
                                          "для добавления комментария к закрытому инциденту. "
                                          "№ инцидента можно узнать из отчета по команде /report")
                logging.info("<- add_comment: Нет открытых инцидентов, введите /add [номер_инцидента] [комментарий] ")
                return
            if ki_current_status == 0:
                bot.send_message(chat_id, "Введите /add [комментарий] "
                                          "для добавления комментария к текущему инциденту.")
                logging.info("<- add_comment: Введите /add [комментарий]")
                return

        num = ki_current_id

        # текущий инцидент открыт, добавляем комментарий к нему
        if ki_current_status == 0:
            if len(msg[0]) > 1:
                num = ki_current_id
                msg.pop(0)
            else:
                bot.send_message(chat_id, "/add [комментарий] - добавить комментарий к текущему инциденту")
                logging.info("<- add_comment: /add [комментарий] - добавить комментарий к текущему инциденту")
                return
        elif ki_current_status == 1:  # текущий закрыт - проверяем, чтобы сообщение было корректно составлено
            if len(msg) > 2:
                if not (msg[1]).isdigit():
                    bot.send_message(chat_id, "Номер инцидента должен быть числом")
                    logging.info("<- add_comment: Номер инцидента должен быть числом")
                    return

                if num > ki_current_id or num < 1:
                    bot.send_message(chat_id, "Некорректный номер инцидента. "
                                              "№ инцидента можно узнать из отчета по команде /report")
                    logging.info("<- add_comment: Некорректный номер инцидента " + str(num) + ", чат:" + str(chat_id))
                    return

                num = int(msg[1])
                # удалим из сообщения команду и номер комментария
                msg.pop(0)
                msg.pop(0)

                check_and_save_hashword(msg, 2, ki_current_id)

            else:
                bot.send_message(chat_id, "/add [номер_инцидента] [комментарий] "
                                          "для добавления комментария к закрытому инциденту. ")
                logger.info("<- add_comment: /add [номер_инцидента] [комментарий]: " + str(chat_id))
                return

        logger.debug("   текущий номер инцидента: " + str(num))

        timestamp = datetime.datetime.now()

        comment = ' '.join(msg)
        datatuple = (num,
                     timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                     username,
                     comment)
        logger.debug("   пишем в БД:\n" + str(datatuple))
        write_to_db("INSERT INTO incident_comment_data(fk_id,comment_time,commentator,comment) VALUES(?,?,?,?);",
                    datatuple)
    # photo передано
    else:
        if len(msg) == 1:
            if ki_current_status == -1 or ki_current_status == 1:
                bot.send_message(chat_id,
                                 "Нет открытых инцидентов, введите /add [номер_инцидента] [опционально: комментарий] "
                                 "для добавления скриншота с комментарием к закрытому инциденту. "
                                 "№ инцидента можно узнать из отчета по команде /report. "
                                 "Скриншоты можно добавлять только по одному.")
                logger.info("<- add_comment: Нет открытых инцидентов...")
                return
            # есть открытый инцидент, добавляем картинку к нему
            else:
                msg = ""
                num = ki_current_id
        elif len(msg) >= 2:
            if ki_current_status == -1 or ki_current_status == 1:
                if not (msg[1]).isdigit():
                    bot.send_message(chat_id, "Номер инцидента должен быть числом")
                    logger.info("<- add_comment: Номер инцидента должен быть числом")
                    return
                num = int(msg[1])
                # удалим из сообщения команду и номер комментария
                msg.pop(0)
                msg.pop(0)
            else:
                num = ki_current_id
                msg.pop(0)

        comment = ' '.join(msg)

        timestamp = datetime.datetime.now()

        data = pickle.dumps(photo)

        datatuple = (num,
                     timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                     username,
                     comment,
                     data)
        logger.debug("  пишем в БД (без фото):\n" +str(datatuple[0]) + " " +
                     str(datatuple[1]) + " " + str(datatuple[2]) + " " + str(datatuple[3]))
        write_to_db(
            "INSERT INTO incident_comment_data(fk_id,comment_time,commentator,comment,data) VALUES(?,?,?,?,?);",
            datatuple)
        logger.info("<- add_comment")
    return

# Создаем экземпляр бота
logger.info("Инициализация объекта TeleBot")
bot = telebot.TeleBot(config['DEFAULT']['APIKEY'])

# сформировать отчет по инциденту в csv
@bot.message_handler(commands=["csv_report"])
def incident_csv_report_command(message):
    logger.info("-> send_report_command")
    save_incident_report_to_csv()
    try:
        f = open("output.csv", "rb")
        bot.send_document(message.chat.id, f)
    except OSError as error:
        bot.send_message(message.chat.id, "Произошла внутренняя ошибка формирования отчета")
        logger.error(error)
        frame = traceback.extract_tb(sys.exc_info()[2])
        line_no = str(frame[0]).split()[4]
        error_log(line_no)
    logger.info("<- send_report_command")
    return


# Метод /commands
@bot.message_handler(commands=["commands"])
def commands_command(message):
    logger.info("-> commands_command")
    bot.send_message(message.chat.id, "***1056bot*** 0.24\n\nБот для оповещения и ведения статистики инцидентов.\n"
                                      "/open [ПЦЛ/УВР] [комментарий] - открыть инцидент по системе [ПЦЛ/УВР], указать комментарий о происшествии\n"
                                      "/close [комментарий] - сообщить о закрытии инцидента\n"
                                      "/report - вывести отчет по инцидентам\n"
                                      "/add [комментарий] - добавить комментарий по текущему (открытому) инциденту\n"
                                      "/add [номер_инцидента] [комментарий] - добавить комментарий по инциденту "
                                      "с указанным номером. Номер можно узнать по команде /report\n"
                                      "/addimg [комментарий] - добавить изображение и комментарий (необязатально)"
                                      " по текущему инциденту. Можно добавлять изображения только по одному!\n"
                                      "/addimg [номер_инцидента] [комментарий] - добавить изображение и комментарий "
                                      "(необзятально) по закрытому инциденту. Можно добавлять изображения только по одному!\n"
                                      "/comments - вывести все комментарии по текущему (открытому инциденту)\n"
                                      "/comments [номер] - вывести комментарии по инциденту [номер]\n"
                                      "/msg [сообщение] - отправить сообщение по всем каналам присутствия бота\n"
                                      "TODO: получение отчета по инцидентам в формате Excel и сохранение "
                                      "медиа комментариев (в т.ч. скриншотов)")
    logger.info("<- commands_command")
    return


# Метод регистрации бота на канале /register
@bot.message_handler(commands=["register"])
def register_command(message):
    global bot_chat_list
    logger.info("-> register_command")
    if message.chat.id in bot_chat_list:
        bot.send_message(message.chat.id, "Бот уже зарегистрирован на канале")
        logger.debug("   Бот уже зарегистрирован на канале " + str(message.chat.id))
        return
    logger.debug("   Дополним список чатов " + str(message.chat.id))
    bot_chat_list.append(message.chat.id)
    bot.send_message(message.chat.id, "Бот успешно зарегистрирован на канале")
    # важно записать chat_id для нового канала
    logger.debug("   запишем id, status: " + str(ki_current_id) + " " + str(ki_current_status))
    store_incident_status()
    logger.debug("<- register_command")
    return


# Метод снятия регистрации бота на канале, поддержка /unregister
@bot.message_handler(commands=["unregister"])
def unregister_command(message):
    logger.info("-> unregister_command")
    unregister(message.chat.id)
    # важно записать chat_id для нового канала
    logger.debug("   запишем id, status: " + str(ki_current_id) + " " + str(ki_current_status))
    store_incident_status()
    logger.info("<- unregister_command")
    return


# реализация снятия регистрации бота на канале
def unregister(chatid):
    logger.info("-> delete_registration")
    global bot_chat_list
    if chatid in bot_chat_list:
        logger.debug("   удаляем: " + str(chatid))
        bot_chat_list.remove(chatid)
    logger.info("-> delete_registration")
    return


# Метод обрабатывающая команду /msg, реализация оповещения по всем каналам присутствия бота
@bot.message_handler(commands=["msg"])
def msg_command(message):
    global bot_chat_list

    logger.info("-> msg_command")
    msg = str(message.text).split()
    if len(msg) == 1:
        bot.send_message(message.chat.id, "/msg [сообщение] для отправки на каналы присутствия бота")
        logger.info("<- msg_command: /msg [сообщение] для отправки на каналы присутствия бота")
        return

    # подготовим сообщение
    msg.pop(0)
    msg = ' '.join(msg)

    initiator = message.from_user.username
    for chat_id in bot_chat_list:
        # в текущий чат не направляем собственное сообщение
        if chat_id == message.chat.id:
            continue
        bot.send_message(chat_id, "Сообщение от @" + str(initiator) + ": " + str(msg))
        logger.debug("   msg в chat_id: " + str(chat_id) + " Сообщение от @" + str(initiator) + ": " + str(msg))
        time.sleep(0.1)

    logger.info("<- msg_command")
    return


# Функция, обрабатывающая команду /open
@bot.message_handler(commands=["open"])
def open_incident_command(message):
    global ki_current_id
    global ki_current_status
    global bot_chat_list

    logger.info("-> open_command")
    # флаг системы для инцидента, чтобы можно было в отчете разделить ПЦЛ/УВР. 0 = ПЦЛ, 1 = УВР
    # атрибут пишется в таблицу по каждому инциденту
    system = 0

    # обновим статус по инциденту
    logger.debug("   загрузка текущих статусов инцидента")
    get_incident_status()

    if ki_current_status == 0:  # последний инцидент не закрыт - значит открывать нельзя
        bot.send_message(message.chat.id, "Необходимо сначала закрыть инцидент №" + str(ki_current_id))
        logger.info("<- open_command: Необходимо сначала закрыть инцидент №" + str(ki_current_id))
        return

    logger.debug("   Исходная команда: " + str(message))
    msg = str(message.text).split()
    if len(msg) < 3:
        bot.send_message(message.chat.id, "/open [ПЦЛ/УВР] [описание] инцидента")
        logger.info("<- open_command: /open [ПЦЛ/УВР] [описание] инцидента")
        return

    if msg[1].upper() == "ПЦЛ" or msg[1].upper() == "PCL" or msg[1] == "0":
        system = 0
    elif msg[1].upper() == "УВР" or msg[1].upper() == "УВР" or msg[1] == "1":
        system = 1
    else:
        bot.send_message(message.chat.id, "/open [ПЦЛ/УВР] [описание] инцидента")
        logger.info("<- open_command: /open [ПЦЛ/УВР] [описание] инцидента")
        return

    # подготовим данные для инициации инцидента из сообщения
    msg.pop(0)
    msg.pop(0)

    # check_and_save_hashword(msg, 1, ki_current_id)

    ki_message = ' '.join(msg)
    timestamp = datetime.datetime.now()
    logger.debug("   Пишем в БД: " + str(timestamp.strftime("%Y-%m-%d %H:%M:%S")) + " " + str(ki_message))

    datatuple = (timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                 message.from_user.username,
                 ki_message,
                 0,
                 system)
    logger.debug("   пишем в БД: " + str(datatuple))
    write_to_db("INSERT INTO bot_chat_log_ext(open_time, initiator, ki_open_info, status, system) VALUES(?,?,?,?,?);",
                datatuple)

    # сохраним параметры
    ki_current_id += 1
    ki_current_status = 0
    logger.debug("   запишем id, status: " + str(ki_current_id) + " " + str(ki_current_status))
    store_incident_status()

    if message.chat.id not in bot_chat_list:
        bot_chat_list.append(message.chat.id)

    for chatid in bot_chat_list:
        bot.send_message(chatid, "Открыт инцидент №" + str(ki_current_id) + ": " + ki_message +
                         "\nИнициатор: @" + str(message.from_user.username))
        logger.debug("   open: " + str(chatid) + "Открыт инцидент №" + str(ki_current_id) + ": " + ki_message +
                         "\nИнициатор: @" + str(message.from_user.username))
        time.sleep(0.1)
    logger.info("<- open_command")
    return


# закрыть инцидент командной /close комменатрий_по_закрытию
@bot.message_handler(commands=["close"])
def close_incident_command(message):
    global ki_current_id
    global ki_current_status
    global bot_chat_list

    logger.info("-> close_command")
    # обновим статус по инциденту
    logger.debug("   загрузим статусы инцидентов")
    get_incident_status()

    if ki_current_status == 1:  # последний инцидент закрыт - значит нечего закрывать
        bot.send_message(message.chat.id, "Последний инцидент закрыт")
        logger.info("<- close_command: Последний инцидент закрыт")
        return
    logger.debug("   Исходная команда: " + str(message))
    msg = str(message.text).split()
    if len(msg) == 1:
        bot.send_message(message.chat.id, "/close комментарий по закрытию инцидента")
        return

    timestamp = datetime.datetime.now()

    # подготовим комментарий по закрытию
    msg.pop(0)
    ki_close_info = ' '.join(msg)

    if message.chat.id not in bot_chat_list:
        bot_chat_list.append(message.chat.id)

    # так как мы закрываем инцидент, соответствующим образом устанавливаем статус инцидента
    ki_current_status = 1

    datatuple = (timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                 message.from_user.username,
                 ki_close_info,
                 ki_current_status,
                 ki_current_id)

    logger.debug("   пишем в БД: " + str(datatuple))

    write_to_db("UPDATE bot_chat_log_ext SET close_time=?, close_manager=?, "
                "ki_close_info=?, status=? WHERE id=?;", datatuple)

    for chatid in bot_chat_list:
        bot.send_message(chatid, "Закрываем инцидент №" + str(ki_current_id) + ": " + ki_close_info +
                         "\nИнцидент закрыл: @" + message.from_user.username +
                         get_incident_report(ki_current_id))
        logger.debug("   close: " + str(chatid) + " Закрываем инцидент №" + str(ki_current_id) + ": " + ki_close_info +
                         "\nИнцидент закрыл: @" + message.from_user.username +
                     get_incident_report(ki_current_id))
        time.sleep(0.1)

    # сохраним изменения по статусу последнего инцидента в БД
    store_incident_status()
    return


# добавить комментарий к текущему инциденту командой /add
@bot.message_handler(commands=["add"])
def add_incident_comment_command(message):
    logger.info("-> add_command")
    add_incident_comment(message.text.split(),
                         message.chat.id,
                         message.from_user.username)
    logger.info("<- add_command")
    return


# добавить комментарий с картинкой. Картинку храним со всеми потрохами, в формате telebot
@bot.message_handler(content_types=["photo"])
def addimg_command(message):
    logger.info("-> addimg_command")
    # проблема - мы можем записать только ту фотографию, под которой есть caption. Когда мы кидаем несколько фоток,
    # запишем только одну с полем caption. Ограничение и нюанс..
    if message.caption is None:
        logger.debug("   add_img: отсутствует caption к фото, пропускаем, отправил " + str(message.from_user.username))
        return
    # а с Caption же работаем
    msg = message.caption.split()
    add_incident_comment(msg,
                         message.chat.id,
                         message.from_user.username,
                         message.photo)
    logger.info("<- addimg_command")
    return


# получение отчета по инцидентам из БД
@bot.message_handler(commands=["report"])
def report_incident_command(message):
    logger.info("-> report_command")
    rep = get_incident_report()
    bot.send_message(message.chat.id, rep)
    logger.debug(rep)
    logger.info("<- report_command")
    return


# получение комментариев к текущему инциденту или инциденту №
@bot.message_handler(commands=["comments"])
def comments_incident_command(message):
    logger.info("-> comments_command")
    global ki_current_id
    global ki_current_status

    get_incident_status()

    curr = -1

    msg = str(message.text).split()
    # paranoid
    if len(msg) == 1 and ki_current_status == -1:
        bot.send_message(message.chat.id,
                         "Нет открытого инцидента, напишите /comments [номер]"
                         " для получения комментариев по прошлому инциденту")
        logger.info("<- comments_command: Нет открытого инцидента")
        return

    if len(msg) == 1 and ki_current_status == 1:
        bot.send_message(message.chat.id,
                         "Нет открытого инцидента, напишите /comments [номер] "
                         "для получения комментариев по прошлому инциденту")
        logger.info("<- comments_command: Нет открытого инцидента")
        return

    if len(msg) == 1:
        if ki_current_status == 0:
            if ki_current_id > 0:
                curr = ki_current_id
            else:
                bot.send_message(message.chat.id,
                                 "Нет открытого ицнидента, напишите /comments #инцидента для получения комментариев "
                                 "по прошлому инциденту")
                logger.info("<- comments_command: Нет открытого инцидента")
                return

    elif len(msg) > 0:
        if not str(msg[1]).isdigit():
            bot.send_message(message.chat.id, "Номер инцидента должен быть числом")
            logger.info("<- comments_command: Номер инцидента должен быть числом")
            return
        else:
            curr = int(msg[1])

    sqlite_connection = 0
    try:
        datatuple = (curr, )
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        cursor = sqlite_connection.cursor()
        cursor.execute("SELECT * FROM incident_comment_data where fk_id=?", datatuple)
        record = cursor.fetchall()

        comment = "Комментарии по инциденту №" + str(curr) + ":"
        if len(record) == 0:
            comment += " отсутствуют, либо указан некорректный номер инцидента"
        else:
            i = 0
            for s in record:
                i += 1
                comment += "\n" + str(i) + ": " + str(s[2]) + " @" + str(s[3]) + " " + str(s[4])
        bot.send_message(message.chat.id, comment)
        logger.debug("   " + comment)
    except sqlite3.Error as error:
        logger.error(error)
        frame = traceback.extract_tb(sys.exc_info()[2])
        line_no = str(frame[0]).split()[4]
        error_log(line_no)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            logger.debug("Соединение с SQLite закрыто")
    logger.info("<- comments_command")
    return


# метод формирует статистику по инцидентам
def get_incident_weekly_stats():
    sqlite_connection = 0
    stats = ""
    logger.info("-> get_stats")
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')

        # получим инцидентов всего по ПЦЛ за 7 дней
        cursor = sqlite_connection.cursor()
        cursor.execute("select count(*) from bot_chat_log_ext WHERE "
                       "open_time > (SELECT DATETIME('now', '-7 day')) and system =0")
        res = cursor.fetchall()
        stats += "Всего инцидентов по ИС ПЦЛ за неделю: " + str(res[0][0]) + "\n"

        # получим инцидентов всего по УВР за 7 дней
        cursor = sqlite_connection.cursor()
        cursor.execute("select count(*) from bot_chat_log_ext WHERE "
                       "open_time > (SELECT DATETIME('now', '-7 day')) and system =1")
        res = cursor.fetchall()
        stats += "Всего инцидентов по ИС УВР за неделю: " + str(res[0][0]) + "\n"

        # получим инцидентов всего
        cursor = sqlite_connection.cursor()
        cursor.execute("select count(*) from bot_chat_log_ext")
        res = cursor.fetchall()

        cursor = sqlite_connection.cursor()
        cursor.execute("select date(open_time) from bot_chat_log_ext where id=1")
        res1 = cursor.fetchall()

        stats += "Всего инцидентов зафиксировано c" + str(res1[0][0]) + ": " + str(res[0][0]) + "\n"

        # получим инцидентов всего по ПЦЛ
        cursor = sqlite_connection.cursor()
        cursor.execute("select count(*) from bot_chat_log_ext where system=0")
        res = cursor.fetchall()
        stats += "* из них по ИС ПЦЛ: " + str(res[0][0]) + "\n"

        # получим инцидентов всего по УВР
        cursor = sqlite_connection.cursor()
        cursor.execute("select count(*) from bot_chat_log_ext where system=1")
        res = cursor.fetchall()
        stats += "* из них по ИС УВР: " + str(res[0][0]) + "\n"

        # получим среднюю длительность решения инцидента
        cursor = sqlite_connection.cursor()
        cursor.execute("select CAST(avg(avg_time) as INTEGER) from "
                       "(select ROUND((julianday(close_time)-julianday(open_time))*24*60) "
                       "as avg_time from bot_chat_log_ext)")
        res = cursor.fetchall()
        stats += "* средняя длительность инцидента: " + str(res[0][0]) + " минут\n"
        """
        select count(*) from bot_chat_log_ext;
        select count(*) from bot_chat_log_ext where system=1;
        select count(*) from bot_chat_log_ext where system=0;
        select count(*) from bot_chat_log_ext WHERE open_time > (SELECT DATETIME('now', '-7 day'));
        select count(*) from bot_chat_log_ext WHERE open_time > (SELECT DATETIME('now', '-7 day')) and system =0;
        select count(*) from bot_chat_log_ext WHERE open_time > (SELECT DATETIME('now', '-7 day')) and system =1;
        select avg(avg_time) from (select ROUND((julianday(close_time)-julianday(open_time))*24*60) as avg_time from bot_chat_log_ext) 
        """
    except sqlite3.Error as error:
        logger.error(error)
        frame = traceback.extract_tb(sys.exc_info()[2])
        line_no = str(frame[0]).split()[4]
        error_log(line_no)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            logger.debug("Соединение с SQLite закрыто")
    logger.info("<- get_stats")
    return stats


# команда /stats формирует статистику по инцидентам всего/за 7 последних дней
@bot.message_handler(commands=["stats"])
def stats_incident_command(message):
    logger.info("-> stats_command")
    stats = "Статистика по инцидентам за неделю:\n"
    stats += get_incident_weekly_stats()
    bot.send_message(message.chat.id, stats)
    logger.info("<- stats_command")
    return


#############################################################################
## Секция загрузки
#############################################################################
# обновим статус
logger.info("Загружаем начальные параметры")
get_incident_status()


# событие отправки статистики инцидентов по расписанию, дергается шедулером
def send_incident_stats():
    global bot_chat_list
    logger.info("-> СОБЫТИЕ send_stats")
    stat = "Еженедельная рассылка по инцидентам:\n"
    stat += get_incident_weekly_stats()
    for chat_id in bot_chat_list:
        bot.send_message(chat_id, stat)
        logger.debug("  рассылка по расписанию: " + str(chat_id))
    logger.info("<- send_stats")


# еженедельный отчет по статистике инцидентов, установка шедулера
def do_schedule():
    logger.info("--> ПОТОК do_schedule")
    schedule.every().monday.at("08:30").do(send_incident_stats)
    while True:
        schedule.run_pending()
        time.sleep(1)


#  поток для заданий
# threading.Thread(target=do_schedule).start()




## SELECT * FROM bot_chat_log_ext WHERE open_time > (SELECT DATETIME('now', '-20 day'))

# Запускаем бота
logger.info("Стартуем..")
bot.polling(timeout=20, long_polling_timeout=5)
