import telebot
import sqlite3
import datetime

# добавить поддержку записи в БД

# Создаем экземпляр бота
bot = telebot.TeleBot('5292665914:AAHN-lYNur-Mr7sC2kGxLmNkkm2BjRcl7MI')
# Функция, обрабатывающая команду /start
@bot.message_handler(commands=["start"])
def start(m, res=False):
    bot.send_message(m.chat.id, 'Я на связи. Напиши мне что-нибудь )')

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
            bot.send_message(m.chat.id, s[1] + " " + s[2])
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
        timestamp = datetime.datetime.now()
        print("пишем в БД: " + message.text + " " + timestamp.strftime("%m/%d/%Y, %H:%M:%S"))

        datatuple = (timestamp, message.text)
        cursor.execute("INSERT INTO bot_chat_log(text) VALUES(?,?);", datatuple)
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


