#!/usr/bin/env python3


import pymysql
import sys

from threading import Thread

from thrift.transport import TSocket
from thrift.server import TServer

sys.path.append('../thrift/gen-py')

from chatservice import ChatService
from userservice import UserService


class ChatServiceHandler(ChatService.Iface):
    def __init__(self, dbh):
        self._dbh = dbh

    def startConnection(self):
        pass

    def endConnection(self):
        pass

    def messagePost(self, chatMessage):
        user = chatMessage.name
        msg = chatMessage.message
        timestamp = chatMessage.timestamp
        with self._dbh.cursor() as cursor:
            sql = 'INSERT INTO messages(user, msg, timestamp) VALUES (%s, %s, %s)'
            cursor.execute(sql, (user, msg, timestamp))

    def messageGet(self, timestamp):
        messages = []
        with self._dbh.cursor() as cursor:
            sql = 'SELECT * FROM messages AS msgs WHERE msgs.timestamp >= %s'
            cursor.execute(sql, (timestamp,))
            for result in cursor.fetchall():
                msg = ChatService.ChatMessage(result['user'], result['msg'])
                messages.append(msg)
        return messages


class UserServiceHandler(UserService.Iface):
    def __init__(self, dbh):
        self._dbh = dbh

    def getUserList(self):
        userlist = []
        with self._dbh.cursor() as cursor:
            sql = 'SELECT * FROM users'
            cursor.execute(sql)
            for result in cursor.fetchall():
                user = result['id']
                userlist.append(user)
        return userlist


def prepare_mysql_connection():
    dbh = pymysql.connect(
        host='140.114.77.33',
        user='meichu',
        password='meichu13579meichu',
        db='meichu',
        charset='utf8',
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )
    return dbh


def start_chat_service(dbh, port):
    handler = ChatServiceHandler(dbh)
    processor = ChatService.Processor(handler=handler)
    transport = TSocket.TServerSocket(port=port)
    server = TServer.TThreadPoolServer(processor, transport)
    server.serve()


def start_user_service(dbh, port):
    handler = UserServiceHandler(dbh)
    processor = UserService.Processor(handler=handler)
    transport = TSocket.TServerSocket(port=port)
    server = TServer.TThreadPoolServer(processor, transport)
    server.serve()


def main():
    print('Port info: chat: {}, user: {}'.format(9876, 9999))
    portchat = 9876
    portuser = 9999
    dbhc = prepare_mysql_connection()
    dbhu = prepare_mysql_connection()
    chat_thread = Thread(target=start_chat_service, args=[dbhc, portchat])
    user_thread = Thread(target=start_user_service, args=[dbhu, portuser])
    chat_thread.start()
    user_thread.start()


if __name__ == '__main__':
    main()
