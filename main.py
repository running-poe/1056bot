import telebot
import sqlite3
import datetime

# добавить поддержку записи в БД

# Создаем экземпляр бота
bot = telebot.TeleBot('5292665914:AAHN-lYNur-Mr7sC2kGxLmNkkm2BjRcl7MI')
# Функция, обрабатывающая команду /start
@bot.message_handler(commands=["ki"])
def start(message, res=False):
    msg = str(message.text).split()
    if len(msg) == 1:
        bot.send_message(message.chat.id, "/ki описание инцидента")
        return
    initiator = message.from_user.first_name + " " + message.from_user.username
    msg.pop(0)
    ki_message = ' '.join(msg)
    timestamp = datetime.datetime.now()

    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        cursor = sqlite_connection.cursor()
        bot.send_message(message.chat.id, 'Вы написали: ' + message.text)
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS bot_chat_log_ext (id INTEGER PRIMARY KEY, "
            "open_time timestamp, "
            "initiator TEXT NOT NULL, "
            "ki_open_info TEXT NOT NULL,"
            "close_time timestamp,"
            "close_manager TEXT NOT NULL,"
            "ki_close_info TEXT NOT NULL);")
        sqlite_connection.commit()



        print("пишем в БД: " + timestamp.strftime("%m/%d/%Y, %H:%M:%S") + message.text)

        datatuple = (timestamp, message.text)
        cursor.execute("INSERT INTO bot_chat_log(dtime, initiator, text) VALUES(?,?);", datatuple)
        sqlite_connection.commit()

        cursor.execute("SELECT * FROM bot_chat_log")
        record = cursor.fetchall()
        for s in record:
            print(s)
        # bot.send_message(m.chat.id, 'log: ' + s)
        cursor.close()


    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


#получение лога из БД
@bot.message_handler(commands=["getlog"])
def getlog(m, res=False):
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        cursor = sqlite_connection.cursor()
        print("тута")
        cursor.execute("SELECT * FROM bot_chat_log")
        record = cursor.fetchall()
        for s in record:
            print(s)
            bot.send_message(m.chat.id, s[1][:19] + " " + s[2])
        cursor.close()
    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


# Получение сообщений от юзера
@bot.message_handler(content_types=["text"])
def handle_text(message):
    try:
        sqlite_connection = sqlite3.connect('sqlite_python.db')
        cursor = sqlite_connection.cursor()
        bot.send_message(message.chat.id, 'Вы написали: ' + message.text)
        cursor.execute("CREATE TABLE IF NOT EXISTS bot_chat_log (id INTEGER PRIMARY KEY, dtime timestamp, text TEXT NOT NULL);")
        sqlite_connection.commit()

        print("пишем в БД: " + timestamp.strftime("%m/%d/%Y, %H:%M:%S") + message.text)

        datatuple = (timestamp, message.text)
        cursor.execute("INSERT INTO bot_chat_log(dtime, text) VALUES(?,?);", datatuple)
        sqlite_connection.commit()

        cursor.execute("SELECT * FROM bot_chat_log")
        record = cursor.fetchall()
        for s in record:
            print(s)
           # bot.send_message(m.chat.id, 'log: ' + s)
        cursor.close()


    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):

            sqlite_connection.close()
            print("Соединение с SQLite закрыто")

# Запускаем бота
bot.polling(none_stop=True, interval=0)


