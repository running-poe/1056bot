import telebot
import sqlite3
import time
import datetime
import json
import pickle

# Глобальные переменные
# идентификатор текущего открытого инцидента, -1 если нет открытых
ki_current_id = -1

# идентификатор предыдущего открытого инцидента, -1 если ранее не было инцидентов
ki_current_status = -1

# набор id чатов, где зарегистрирован бот
bot_chat_list = []


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
        print("initdb:Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("initdb: соединение с SQLite закрыто")


def get_incident_status():
    global ki_current_id
    global ki_current_status
    global bot_chat_list

    with open('app.json', 'r') as file_object:
        data = json.load(file_object)
        ki_current_id = data[0]
        ki_current_status = data[1]

    # при первом запуске есл
    try:
        with open('chats.json', 'r') as file_object:
            data = json.load(file_object)
            bot_chat_list = data
    except:
        pass


def store_incident_status():
    global ki_current_id
    global ki_current_status
    global bot_chat_list

    dataduple = [ki_current_id, ki_current_status]  # create a set of numbers
    filename = 'app.json'  # use the file extension .json
    with open(filename, 'w') as file_object:  # open the file in write mode
        json.dump(dataduple, file_object)

    filenamechats = 'chats.json'
    with open(filenamechats, 'w') as file_object:  # open the file in write mode
        json.dump(bot_chat_list, file_object)


# функция записи в БД
def write_to_db(str, datatuple):
    sqlite_connection = 0
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        cursor = sqlite_connection.cursor()
        cursor.execute(str, datatuple)
        sqlite_connection.commit()
    except sqlite3.Error as error:
        print("write_to_db: Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("write_to_db: соединение с SQLite закрыто")


def get_report(curr=-1):
    # строка с отчетом
    report = ""
    # если инцидент не закрыт, то времени закрытия не показываем. переменная нужна для отображения статуса закрытия
    result = ""
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
                    print(s)
                    time_start = datetime.datetime.strptime(s[1], "%Y-%m-%d, %H:%M:%S")
                    report += "№" + str(s[0]) + " Открыт: " + str(s[1]) + " " + str(s[3])
                    if int(s[7]) == 0:
                        result = "\n№" + str(s[0]) + " Не закрыт!\n"
                    else:
                        time_end = datetime.datetime.strptime(s[4], "%Y-%m-%d, %H:%M:%S")
                        diff = time_end - time_start
                        print(diff)
                        result = "\n№" + str(s[0]) + " Закрыт: " + str(s[4]) + " " + str(s[6]) + "\n"
                        result += "\nДлительность инцидента: " + str(diff)
                    report += result + "\n==========================\n"

        else:
            if len(record) > 0:
                print(record)
                time_start = datetime.datetime.strptime(record[0][1], "%Y-%m-%d, %H:%M:%S")
                time_end = datetime.datetime.strptime(record[0][4], "%Y-%m-%d, %H:%M:%S")
                diff = time_end - time_start
                print(diff)
                report = "\nДлительность инцидента: " + str(diff)
            else:
                report = "\nДлительность инцидента не определена!"

        cursor.close()
    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")
    return report


# метод добавления комментария
def add_comment(msg, chat_id, username, photo=0):
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
                return
            if ki_current_status == 0:
                bot.send_message(chat_id, "Введите /add [комментарий] "
                                          "для добавления комментария к текущему инциденту.")
                return

        num = ki_current_id

        # текущий инцидент открыт, добавляем комментарий к нему
        if ki_current_status == 0:
            if len(msg[0]) > 1:
                num = ki_current_id
                msg.pop(0)
            else:
                bot.send_message(chat_id, "/add [комментарий] - добавить комментарий к текущему инциденту")
                return
        elif ki_current_status == 1:  # текущий закрыт - проверяем, чтобы сообщение было корректно составлено
            if len(msg) > 2:
                if not (msg[1]).isdigit():
                    bot.send_message(chat_id, "Номер инцидента должен быть числом")
                    return

                if num > ki_current_id or num < 1:
                    bot.send_message(chat_id, "Некорректный номер инцидента. "
                                              "№ инцидента можно узнать из отчета по команде /report")
                    return

                num = int(msg[1])
                # удалим из сообщения команду и номер комментария
                msg.pop(0)
                msg.pop(0)
            else:
                bot.send_message(chat_id, "/add [номер_инцидента] [комментарий] "
                                          "для добавления комментария к закрытому инциденту. ")
                return

        print("add: номер инцидента ", num)

        timestamp = datetime.datetime.now()

        comment = ' '.join(msg)
        datatuple = (num,
                     timestamp.strftime("%Y-%m-%d, %H:%M:%S"),
                     username,
                     comment)
        print("add ", datatuple)
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
                return
            # есть открытый инцидент, добавляем картинку к нему
            else:
                msg = ""
                num = ki_current_id
        elif len(msg) >= 2:
            if ki_current_status == -1 or ki_current_status == 1:
                if not (msg[1]).isdigit():
                    bot.send_message(chat_id, "Номер инцидента должен быть числом")
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
                     timestamp.strftime("%Y-%m-%d, %H:%M:%S"),
                     username,
                     comment,
                     data)
        print("add ", datatuple)
        write_to_db(
            "INSERT INTO incident_comment_data(fk_id,comment_time,commentator,comment,data) VALUES(?,?,?,?,?);",
            datatuple)


# Создаем экземпляр бота и иниицируем db
bot = telebot.TeleBot('5292665914:AAHN-lYNur-Mr7sC2kGxLmNkkm2BjRcl7MI')


# init_db()

# Функция /commands
@bot.message_handler(commands=["commands"])
def commands_command(message, res=False):
    bot.send_message(message.chat.id, "***1056bot*** 0.16a\n\nБот для оповещения и ведения статистики инцидентов.\n"
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
    return


# Функция регистрации бота на канале /register
@bot.message_handler(commands=["register"])
def register_command(message, res=False):
    global bot_chat_list
    if message.chat.id in bot_chat_list:
        bot.send_message(message.chat.id, "Бот уже зарегистрирован на канале")
        return
    bot_chat_list.append(message.chat.id)
    bot.send_message(message.chat.id, "Бот успешно зарегистрирован на канале")
    # важно записать chat_id для нового канала
    store_incident_status()


@bot.message_handler(commands=["unregister"])
def unregister_command(message, res=False):
    delete_registration(message.chat.id)
    # важно записать chat_id для нового канала
    store_incident_status()


def delete_registration(chatid):
    global bot_chat_list
    if chatid in bot_chat_list:
        bot_chat_list.remove(chatid)


# Функция, обрабатывающая команду /msg
@bot.message_handler(commands=["msg"])
def msg(message, res=False):
    global bot_chat_list

    msg = str(message.text).split()
    if len(msg) == 1:
        bot.send_message(message.chat.id, "/msg [сообщение] для отправки на каналы присутствия бота")
        return

    # подготовим сообщение
    msg.pop(0)
    msg = ' '.join(msg)

    initiator = message.from_user.username
    for chat_id in bot_chat_list:
        try:
            bot.send_message(chat_id, "Сообщение от @" + str(initiator) + ": " + str(msg))
            time.sleep(0.1)
        finally:
            print("msg: ошибка ", str(chat_id))
            delete_registration(chat_id)



# Функция, обрабатывающая команду /open
@bot.message_handler(commands=["open"])
def open_command(message, res=False):
    global ki_current_id
    global ki_current_status
    global bot_chat_list

    system = 0

    # обновим статус по инциденту
    get_incident_status()

    if ki_current_status == 0:  # последний инцидент не закрыт - значит открывать нельзя
        bot.send_message(message.chat.id, "Необходимо сначала закрыть инцидент №" + str(ki_current_id))
        return

    msg = str(message.text).split()
    if len(msg) < 3:
        bot.send_message(message.chat.id, "/open [ПЦЛ/УВР] [описание] инцидента")
        return

    if msg[1].upper() == "ПЦЛ" or msg[1].upper() == "PCL" or msg[1] == "0":
        system = 0
    elif msg[1].upper() == "УВР" or msg[1].upper() == "УВР" or msg[1] == "1":
        system = 1
    else:
        bot.send_message(message.chat.id, "/open [ПЦЛ/УВР] [описание] инцидента")
        return

    # подготовим данные для инициации инцидента из сообщения
    msg.pop(0)
    msg.pop(0)
    ki_message = ' '.join(msg)
    timestamp = datetime.datetime.now()
    print("Пишем в БД: " + timestamp.strftime("%Y-%m-%d, %H:%M:%S") + " " + ki_message)

    datatuple = (timestamp.strftime("%Y-%m-%d, %H:%M:%S"),
                 message.from_user.username,
                 ki_message,
                 0,
                 system)

    write_to_db("INSERT INTO bot_chat_log_ext(open_time, initiator, ki_open_info, status, system) VALUES(?,?,?,?,?);",
                datatuple)

    # сохраним параметры
    ki_current_id += 1
    ki_current_status = 0
    store_incident_status()

    print("Номер записи: " + str(ki_current_id))

    if message.chat.id not in bot_chat_list:
        bot_chat_list.append(message.chat.id)

    for chatid in bot_chat_list:
        try:
            print("open chatid: ", chatid)
            bot.send_message(chatid, "Открыт инцидент №" + str(ki_current_id) + ": " + ki_message +
                         "\nИнициатор: @" + str(message.from_user.username))
            time.sleep(0.1)
        finally:
            print("msg: ошибка ", str(chatid))
            delete_registration(chatid)


    return


# закрыть инцидент командной /close комменатрий_по_закрытию
@bot.message_handler(commands=["close"])
def close_command(message, res=False):
    global ki_current_id
    global ki_current_status
    global bot_chat_list

    # обновим статус по инциденту
    get_incident_status()

    if ki_current_status == 1:  # последний инцидент закрыт - значит нечего закрывать
        bot.send_message(message.chat.id, "Последний инцидент закрыт")
        return

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

    datatuple = (timestamp.strftime("%Y-%m-%d, %H:%M:%S"),
                 message.from_user.username,
                 ki_close_info,
                 ki_current_status,
                 ki_current_id
                 )

    print(datatuple)
    print(ki_current_id)
    write_to_db("UPDATE bot_chat_log_ext SET close_time=?, close_manager=?, "
                "ki_close_info=?, status=? WHERE id=?;", datatuple)

    for chatid in bot_chat_list:
        bot.send_message(chatid, "Закрываем инцидент №" + str(ki_current_id) + ": " + ki_close_info +
                         "\nИнцидент закрыл: @" + message.from_user.username +
                         get_report(ki_current_id))
        time.sleep(0.1)

    # сохраним изменения по статусу последнего инцидента в БД
    store_incident_status()
    return


# добавить комментарий к текущему инциденту командой /add
@bot.message_handler(commands=["add"])
def add_command(message, res=False):
    add_comment(message.text.split(),
                message.chat.id,
                message.from_user.username)
    return


# добавить комментарий с картинкой. Картинку храним со всеми потрохами, в формате telebot
@bot.message_handler(content_types=["photo"])
def addimg_command(message):
    msg = [""]

    # проблема - мы можем записать только ту фотографию, под которой есть caption. Когда мы кидаем несколько фоток,
    # запишем только одну с полем caption. Ограничение и нюанс..
    if message.caption is None:
        return

    # а с Caption же работаем
    msg = message.caption.split()
    add_comment(msg,
                message.chat.id,
                message.from_user.username,
                message.photo)
    return


# получение отчета по инцидентам из БД
@bot.message_handler(commands=["report"])
def report_command(message, res=False):
    rep = get_report()
    bot.send_message(message.chat.id, rep)
    return


@bot.message_handler(commands=["comments"])
def comments_command(message, res=False):
    global ki_current_id
    global ki_current_status

    get_incident_status()

    curr = -1

    msg = str(message.text).split()
    # paranoid
    if len(msg) == 1 and ki_current_status == -1:
        bot.send_message(message.chat.id,
                         "Нет открытого инцидента, напишите /comments [номер] для получения комментариев по прошлому инциденту")
        return

    if len(msg) == 1 and ki_current_status == 1:
        bot.send_message(message.chat.id,
                         "Нет открытого инцидента, напишите /comments [номер] для получения комментариев по прошлому инциденту")
        return

    if len(msg) == 1:
        if ki_current_status == 0:
            if ki_current_id > 0:
                curr = ki_current_id
            else:
                bot.send_message(message.chat.id,
                                 "Нет открытого ицнидента, напишите /comments #инцидента для получения комментариев "
                                 "по прошлому инциденту")
                return


    elif len(msg) > 0:
        if not str(msg[1]).isdigit():
            bot.send_message(message.chat.id, "Номер инцидента должен быть числом")
            return
        else:
            curr = int(msg[1])

    try:
        datatuple = (curr,)
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
    except sqlite3.Error as error:
        print("comments: Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("comments: Соединение с SQLite закрыто")


# обновим статус
get_incident_status()

# Запускаем бота
bot.polling(none_stop=True, interval=0)
