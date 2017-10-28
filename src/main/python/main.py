#!/usr/bin/env python3


import pymysql
import requests
import sys

from urllib.parse import urlencode

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
        payload = {'user': user, 'msg': msg}
        payload = urlencode(payload)
        res = requests.get('http://140.114.77.33/msg.php', params=payload)
        if res.status_code != 200:
            print(res.text)

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
    print('Port info: chat: {}, user: {}'.format(9876, 9877))
    portchat = 9876
    portuser = 9877
    dbh = prepare_mysql_connection()
    start_chat_service(dbh, portchat)
    start_user_service(dbh, portuser)


if __name__ == '__main__':
    main()
