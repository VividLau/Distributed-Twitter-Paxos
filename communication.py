from socket import *
import threading
import pickle
import datetime


class Connection:

    def __init__(self):
        self.log = [{'None': 'None'}]
        self.new_log = []
        self.id_self = None
        self.block = [[0 for i in range(5)] for i in range(5)]

        self.port = 8789
        self.host = '0.0.0.0'

        self.failed_site = 0
        self.failed_sock = []

        self.list_listen_sock = []
        self.listensock = socket(AF_INET, SOCK_STREAM)
        self.listensock.bind((self.host, self.port))
        self.listensock.listen(5)

        self.connectsock1 = socket(AF_INET, SOCK_STREAM)
        self.connectsock2 = socket(AF_INET, SOCK_STREAM)
        self.connectsock3 = socket(AF_INET, SOCK_STREAM)
        self.connectsock4 = socket(AF_INET, SOCK_STREAM)
        self.list_connect_sock = \
            [
                self.connectsock1, self.connectsock2, self.connectsock3, self.connectsock4
            ]

        try:
            pkl_file = open('log.pkl', 'rb')
            self.log = pickle.load(pkl_file)
        except OSError as e:
            if e.errno != 2:
                raise e

        self.log.reverse()
        for block_event in self.log:
            if block_event.__contains__('block'):
                self.block[int(block_event['name']) - 1][int(block_event['block']) - 1] = 1
            elif block_event.__contains__('unblock'):
                self.block[int(block_event['name']) - 1][int(block_event['unblock']) - 1] = 0
        self.log.reverse()

        print('Please enter your id: ')
        self.id_self = input('')

    def readconfig(self):
        file = open("config.txt")
        try:
            file_line = file.readlines()
            nodes = [(addr, user_id)
                     for line in file_line
                     for addr, user_id in [line.strip().split(":")]]
        finally:
            file.close()
        self_id = self.id_self
        other_addr = []
        other_id = []
        for node in nodes:
            if node[1] == self_id:
                continue
            other_addr.append(node[0])
            other_id.append(int(node[1]))

        return other_addr, other_id

    def listen_thread(self, client_sock, address):

        while True:
            rough_data = client_sock.recv(2048)

            if rough_data == b'':
                print('client down!')
                self.failed_site += 1
                break
            else:
                encode_data = pickle.loads(rough_data)

                if encode_data.__contains__('block'):
                    self.block[int(encode_data['name']) - 1][int(encode_data['block']) - 1] = 1
                elif encode_data.__contains__('unblock'):
                    self.block[int(encode_data['name']) - 1][int(encode_data['unblock']) - 1] = 0

                print(address[0] + ' say: ' + str(rough_data))
                self.log.insert(0, encode_data)
                log_file_w = open('log.pkl', 'wb')
                pickle.dump(self.log, log_file_w)
                log_file_w.close()

        client_sock.close()
        self.list_listen_sock.remove(client_sock)

    def connected_sock_thread(self, connected_sock, block):

        while True:
            connected_sock.setblocking(block)
            rough_data = connected_sock.recv(2048)

            if rough_data == b'':
                print('server down!')
                self.failed_site += 1
                break
            else:
                encode_data = pickle.loads(rough_data)

                if encode_data.__contains__('block'):
                    self.block[int(encode_data['name']) - 1][int(encode_data['block']) - 1] = 1
                elif encode_data.__contains__('unblock'):
                    self.block[int(encode_data['name']) - 1][int(encode_data['unblock']) - 1] = 0

                print('server say:' + str(rough_data))
                self.log.insert(0, encode_data)
                log_file_w = open('log.pkl', 'wb')
                pickle.dump(self.log, log_file_w)
                log_file_w.close()

        connected_sock.close()
        self.list_connect_sock.remove(connected_sock)

    def listen(self):

        while True:
            client, address = self.listensock.accept()
            print('connected from', address)
            self.failed_site -= 1
            self.list_listen_sock.append(client)
            new_thread = threading.Thread(target=self.listen_thread, args=(client, address))
            new_thread.start()

    def listen_start(self):
        listen_thread = threading.Thread(target=self.listen)
        listen_thread.start()


if __name__ == '__main__':

    cinstance = Connection()
    addr_list, id_list = cinstance.readconfig()
    cinstance.listen_start()

    for sock in cinstance.list_connect_sock:
        loop_index = cinstance.list_connect_sock.index(sock)
        ADDR = (addr_list[loop_index], 8789)
        sock.settimeout(0.5)
        connect_ = sock.connect_ex(ADDR)
        if connect_ != 0:
            sock.close()
            cinstance.failed_sock.append(sock)
            cinstance.failed_site += 1

    for fail in cinstance.failed_sock:
        cinstance.list_connect_sock.remove(fail)

    for sock in cinstance.list_connect_sock:
        connected_thread = threading.Thread(target=cinstance.connected_sock_thread, args=(sock, True))
        connected_thread.start()

    print('Welcome!')
    print('We have ' + str(cinstance.failed_site) + ' downed site(s).')

    while True:
        print('What do you want to do?')
        choice = input('')

        if choice == 'tweet':
            print('Say something: ', end='')
            tweet = input('')
            event = {'name': cinstance.id_self, 'tweet': tweet, 'time': datetime.datetime.now()}
            data = pickle.dumps(event)
            for sock in cinstance.list_listen_sock:
                sock.sendall(data)
            for sock in cinstance.list_connect_sock:
                sock.sendall(data)
            cinstance.log.insert(0, event)
            log_file_w = open('log.pkl', 'wb')
            pickle.dump(cinstance.log, log_file_w)
            log_file_w.close()

        if choice == 'block':
            print('Who do you want to block?')
            block_id = input('')
            cinstance.block[int(cinstance.id_self) - 1][int(block_id) - 1] = 1
            event = {'name': cinstance.id_self, 'block': block_id, 'time': datetime.datetime.now()}
            data = pickle.dumps(event)
            for sock in cinstance.list_listen_sock:
                sock.sendall(data)
            for sock in cinstance.list_connect_sock:
                sock.sendall(data)
            cinstance.log.insert(0, event)
            log_file_w = open('log.pkl', 'wb')
            pickle.dump(cinstance.log, log_file_w)
            log_file_w.close()

        if choice == 'unblock':
            print('Who do you want to unblock?')
            unblock_id = input('')
            cinstance.block[int(cinstance.id_self) - 1][int(unblock_id) - 1] = 0
            event = {'name': cinstance.id_self, 'unblock': unblock_id, 'time': datetime.datetime.now()}
            data = pickle.dumps(event)
            for sock in cinstance.list_listen_sock:
                sock.sendall(data)
            for sock in cinstance.list_connect_sock:
                sock.sendall(data)
            cinstance.log.insert(0, event)
            log_file_w = open('log.pkl', 'wb')
            pickle.dump(cinstance.log, log_file_w)
            log_file_w.close()

        if choice == 'view':
            for tweet in cinstance.log:
                if tweet.__contains__('tweet'):
                    if cinstance.block[int(tweet['name']) - 1][int(cinstance.id_self) - 1] == 1:
                        continue
                    else:
                        print(tweet['name'], tweet['tweet'], str(tweet['time']).rjust(20))

        if choice == 'down':
            print('We have ' + str(cinstance.failed_site) + ' downed site(s).')

        else:
            continue


