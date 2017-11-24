from socket import *
import threading
import pickle
import time
import datetime


class Connection:

    def __init__(self):

        # variable for synod
        self.prepare_response = 0
        self.ack_response = 0
        self.response_value = []
        self.Max_prepare = 0
        self.accNum = 0
        self.accVal = None

        # variable for leader election
        self.leader = False

        self.log = [{'None': 'None'}]
        self.id_self = None
        self.block = [[0 for i in range(5)] for i in range(5)]

        self.port = 8789
        self.host = '0.0.0.0'

        self.failed_site = 0
        self.failed_sock = []

        self.connectsock1 = socket(AF_INET, SOCK_STREAM)
        self.connectsock2 = socket(AF_INET, SOCK_STREAM)
        self.connectsock3 = socket(AF_INET, SOCK_STREAM)
        self.connectsock4 = socket(AF_INET, SOCK_STREAM)
        self.connectsock_self = socket(AF_INET, SOCK_STREAM)
        self.list_connect_sock = \
            [
                self.connectsock1, self.connectsock2, self.connectsock3, self.connectsock4, self.connectsock_self
            ]

        print('Please enter your id: ')
        self.id_self = input('')

        self.list_listen_sock = []
        self.listensock = socket(AF_INET, SOCK_STREAM)
        self.listensock.bind((self.host, self.port))
        self.listensock.listen(5)

        try:
            pkl_file = open('log.pkl', 'rb')
            self.log = pickle.load(pkl_file)
        except OSError as e:
            self.log = [{'Start': 'Site %s started at %s' % (self.id_self, str(datetime.datetime.now()))}]
            if e.errno != 2:
                raise e

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
                self.connectsock_self.connect((node[0], self.port))
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
            decode_data = pickle.loads(rough_data)
            self.synod_accept(client_sock, decode_data)

            # print(address[0] + ' say: ' + str(decode_data))

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

            decode_data = pickle.loads(rough_data)
            self.synod_accept(connected_sock, decode_data)

            # print('server say:' + str(decode_data))

        connected_sock.close()
        self.list_connect_sock.remove(connected_sock)

    def synod_accept(self, client_sock, decode_data):

        # accept msg as a proposer:
        if decode_data.__contains__('promise'):
            self.prepare_response += 1
            self.response_value.append(decode_data)

        elif decode_data.__contains__('ack'):
            self.ack_response += 1

        # accept msg as an acceptor:
        elif decode_data.__contains__('prepare'):
            print('acceptor: receive prepare number %d, max prepare num is %d'
                  % (decode_data['prepare'], self.Max_prepare))
            print()
            if decode_data['prepare'] > self.Max_prepare:
                self.Max_prepare = decode_data['prepare']
                promise = {'promise': 'promise', 'accNum': self.accNum, 'accVal': self.accVal}
                encode_promise = pickle.dumps(promise)
                client_sock.sendall(encode_promise)

        elif decode_data.__contains__('accept'):
            print('acceptor: receive accNum number %d, max prepare num is %d'
                  % (decode_data['accNum'], self.Max_prepare))
            print()
            if decode_data['accNum'] >= self.Max_prepare:
                self.accNum = decode_data['accNum']
                self.accVal = decode_data['accVal']
                ack = {'ack': 'ack', 'accNum': self.accNum, 'accVal': self.accVal}
                encode_ack = pickle.dumps(ack)
                client_sock.sendall(encode_ack)

        elif decode_data.__contains__('commit'):
            print('acceptor: receive commit(v) from proposer')
            print()
            self.log.insert(0, decode_data['value'])
            log_file = open('log.pkl', 'wb')
            pickle.dump(self.log, log_file)
            log_file.close()
            print('Log entry created')
            print()
            self.Max_prepare = 0
            self.accNum = 0
            self.accVal = None

        elif decode_data.__contains__('give_up'):
            print('fail to create this log entry')
            print()
            self.Max_prepare = 0
            self.accNum = 0
            self.accVal = None

    def synod_broadcast(self, broadcast_event):
        # Synod algorithm for proposers
        m = 0
        count = 0

        if self.log[0].__contains__('name') and self.log[0]['name'] == self.id_self:
            self.leader = True
        else:
            self.leader = False

        while True:
            self.prepare_response = 0
            self.ack_response = 0

            # if site is the leader:
            if self.leader is True:
                leader_acc = {'accept': 'accept', 'accNum': 0, 'accVal': broadcast_event}
                print('proposer: send accept(0, v) to all acceptor as a leader')
                print()
                self.broadcast(leader_acc)
                time.sleep(0.1)
                print('proposer: receive %d ack to accept(0, v)' % self.ack_response)
                print()
                if self.ack_response > 2:
                    commit = {'commit': 'commit', 'value': leader_acc['accVal']}
                    print('proposer: sent commit(v) to all acceptors')
                    print()
                    self.broadcast(commit)
                    break
                else:
                    self.ack_response = 0
                    self.leader = False
                    pass

            # prepare phase:
            if self.prepare_response < 3:
                self.prepare_response = 0
                prepareNum = m * 5 + int(self.id_self)
                prepare = {'prepare': prepareNum}
                print('proposer: send prepareNum: %d to all acceptors' % prepareNum)
                print()
                self.broadcast(prepare)
                time.sleep(0.1)
                print('proposer: received %d response(s) to prepare' % self.prepare_response)
                print()
                count += 1
                m += 1
                if count > 5:
                    give_up = {'give_up': 'give_up'}
                    self.broadcast(give_up)
                    break

            # propose phase:
            if self.prepare_response > 2:

                max_list = []
                for value in self.response_value:
                    max_list.append(value['accNum'])
                max_accNum = max(max_list)
                for value in self.response_value:
                    if value['accNum'] == max_accNum:
                        max_accVal = value['accVal']
                        break

                if max_accVal is None:
                    accept = {'accept': 'accept', 'accNum': prepareNum, 'accVal': broadcast_event}
                else:
                    accept = {'accept': 'accept', 'accNum': prepareNum, 'accVal': max_accVal}

                print('proposer: sent accept(n, v) to all acceptors')
                print()
                self.broadcast(accept)
                time.sleep(0.1)
                print('proposer: received %d ack to accept(n, v)' % self.ack_response)
                print()
                # send commit to acceptors who will write the log
                if self.ack_response > 2:
                    commit = {'commit': 'commit', 'value': accept['accVal']}
                    print('proposer: sent commit(v) to all acceptors')
                    print()
                    self.broadcast(commit)
                    break

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

    def broadcast(self, bcast_event):
        bcast_data = pickle.dumps(bcast_event)
        for bcast_sock in self.list_listen_sock:
            bcast_sock.sendall(bcast_data)
        for bcast_sock in self.list_connect_sock:
            if bcast_sock == self.connectsock_self:
                continue
            bcast_sock.sendall(bcast_data)


if __name__ == '__main__':

    cinstance = Connection()
    addr_list, id_list = cinstance.readconfig()
    cinstance.listen_start()

    for sock in cinstance.list_connect_sock:
        if sock == cinstance.connectsock_self:
            continue
        loop_index = cinstance.list_connect_sock.index(sock)
        ADDR = (addr_list[loop_index], 8789)
        sock.settimeout(0.2)
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
        time.sleep(0.5)
        print('What do you want to do?')
        choice = input('')

        if choice == 'tweet':
            print('Say something: ', end='')
            tweet = input('')
            t_time = datetime.datetime.utcnow()
            t_event = {'name': cinstance.id_self, 'tweet': tweet, 'time': str(t_time)}
            cinstance.synod_broadcast(t_event)

        if choice == 'block':
            print('Who do you want to block?')
            block_id = input('')
            b_time = datetime.datetime.utcnow()
            b_event = {'name': cinstance.id_self, 'block': block_id, 'time': str(b_time)}
            cinstance.synod_broadcast(b_event)

        if choice == 'unblock':
            print('Who do you want to unblock?')
            unblock_id = input('')
            u_time = datetime.datetime.utcnow()
            u_event = {'name': cinstance.id_self, 'unblock': unblock_id, 'time': str(u_time)}
            cinstance.synod_broadcast(u_event)

        if choice == 'view':

            cinstance.log.reverse()
            for block_event in cinstance.log:
                if block_event.__contains__('block'):
                    cinstance.block[int(block_event['name']) - 1][int(block_event['block']) - 1] = 1
                elif block_event.__contains__('unblock'):
                    cinstance.block[int(block_event['name']) - 1][int(block_event['unblock']) - 1] = 0
            cinstance.log.reverse()

            for tweet in cinstance.log:
                if tweet.__contains__('tweet'):
                    if cinstance.block[int(tweet['name']) - 1][int(cinstance.id_self) - 1] == 1:
                        continue
                    else:
                        print(tweet['name'], tweet['tweet'], str(tweet['time']).rjust(30))

        if choice == 'down':
            print('We have ' + str(cinstance.failed_site) + ' downed site(s).')

        if choice == 'log':
            for logs in cinstance.log:
                print(logs)

        if choice == 'test':
            event = {'name': cinstance.id_self, 'tweet': 'tweet', 'time': datetime.datetime.now()}
            data = pickle.dumps(event)
            print(type(data))

        else:
            continue


