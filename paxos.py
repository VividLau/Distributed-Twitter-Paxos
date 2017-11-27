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
        self.ack_value = []

        # variable for learning missed log
        self.logid = []

        # Max_prepareNum, accNum and accVal
        self.paxos_variable = [0, 0, None]

        # variable for leader election
        self.leader = False
        
        # variable for update
        self.is_updated = True

        self.log = []
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
            pkl_file.close()

            pkl_variable = open('variable.pkl', 'rb')
            self.paxos_variable = pickle.load(pkl_variable)
            pkl_variable.close()

        except OSError as e:
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

    def listen_thread(self, client_sock):

        while True:
            rough_data = client_sock.recv(2048)
            if rough_data == b'':
                print('client down!')
                self.failed_site += 1
                break
            decode_data = pickle.loads(rough_data)
            self.synod_accept(client_sock, decode_data)

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

        connected_sock.close()
        self.list_connect_sock.remove(connected_sock)

    def synod_accept(self, client_sock, decode_data):

        # initialize Max_prepareNum, accNum and accVal
        Max_prepare = self.paxos_variable[0]
        accNum = self.paxos_variable[1]
        accVal = self.paxos_variable[2]

        # accept msg as a proposer:
        if decode_data.__contains__('promise'):
            self.prepare_response += 1
            self.response_value.append(decode_data)

        elif decode_data.__contains__('ack'):
            self.ack_response += 1
            self.ack_value.append(decode_data['accVal'])

        elif decode_data.__contains__('reply'):
            self.logid.append(decode_data['reply'])

        # accept msg as an acceptor:
        elif decode_data.__contains__('ask'):
            print('acceptor: proposer ask for max log id')
            print()
            log_lenth = len(self.log)
            reply = {'reply': log_lenth}
            encode_reply = pickle.dumps(reply)
            client_sock.sendall(encode_reply)
            print('acceptor: send max log id %d to proposer' % log_lenth)
            print()

        elif decode_data.__contains__('prepare'):
            print('acceptor: receive prepare number %d, max prepare num is %d'
                  % (decode_data['prepare'], Max_prepare))
            print()
            if decode_data['prepare'] > Max_prepare:
                self.paxos_variable[0] = decode_data['prepare']

                # store Paxos variable!
                self.store_variable(self.paxos_variable)
                a = open('variable.pkl', 'rb')
                aa = pickle.load(a)
                a.close()
                print('.....//////////')
                print(aa)
                print('-----//////////')

                promise = {'promise': 'promise', 'accNum': accNum, 'accVal': accVal}
                encode_promise = pickle.dumps(promise)
                print('acceptor: send promise to proposer')
                print('accNum is ' + str(accNum) + ', accVal is ' + str(accVal))
                print()
                client_sock.sendall(encode_promise)

        elif decode_data.__contains__('accept'):
            print('acceptor: receive accNum number %d, max prepare num is %d'
                  % (decode_data['accNum'], Max_prepare))
            print()
            if decode_data['accNum'] >= Max_prepare:
                self.paxos_variable[1] = decode_data['accNum']
                self.paxos_variable[2] = decode_data['accVal']

                # store paxos variable!
                self.store_variable(self.paxos_variable)
                a = open('variable.pkl', 'rb')
                aa = pickle.load(a)
                a.close()
                print('=================')
                print(aa)
                print('=================')

                if decode_data.__contains__('update'):
                    try:
                        ack = {'ack': 'ack', 'accNum': self.paxos_variable[1],
                               'accVal': self.log[decode_data['update'] - 1]}
                    except IndexError:
                        ack = {'ack': 'ack', 'accNum': self.paxos_variable[1],
                               'accVal': None}
                else:
                    ack = {'ack': 'ack', 'accNum': self.paxos_variable[1], 'accVal': self.paxos_variable[2]}
                encode_ack = pickle.dumps(ack)
                print('acceptor: send ack(accNum, accVal) to proposer')
                print('accNum is %d, accVal is %s' % (ack['accNum'], str(ack['accVal'])))
                print()
                client_sock.sendall(encode_ack)

        elif decode_data.__contains__('commit'):

            created = True
            v = decode_data['value']
            ID = v['logid']
            print('acceptor: receive commit(v) from proposer')
            print('v is ' + str(v))
            print()
            if ID > len(self.log) + 1:
                for i in range(ID - len(self.log) - 1):
                    self.log.append(None)
                self.log.append(v)
            elif ID == len(self.log) + 1:
                self.log.append(v)
                print('write ' + str(v) + ' into log')
                print()
            else:
                if self.log[ID - 1] is None:
                    self.log[ID - 1] = v
                    print('write ' + str(v) + ' into log')
                    print()
                elif self.log[ID - 1] == v:
                    print('log already exist')
                    print()
                    created = False
                else:
                    v['logid'] = len(self.log)
                    self.log.append(v)
                    
            log_file = open('log.pkl', 'wb')
            pickle.dump(self.log, log_file)
            log_file.close()
            if created is True:
                print('Log entry created')
                print()
            self.paxos_variable = [0, 0, None]

            # store Paxos variable!
            self.store_variable(self.paxos_variable)

            a = open('variable.pkl', 'rb')
            aa = pickle.load(a)
            a.close()
            print('---------------')
            print(aa)
            print('---------------')

        elif decode_data.__contains__('give_up'):
            print('fail to create this log entry')
            print()
            self.paxos_variable = [0, 0, None]

            # store Paxos variable!
            self.store_variable(self.paxos_variable)

    def synod_broadcast(self, broadcast_event, learning=False):

        # Synod algorithm for proposers
        m = 0
        count = 0

        try:
            if self.log[-1].__contains__('name') and self.log[-1]['name'] == self.id_self:
                self.leader = True
            else:
                self.leader = False
        except IndexError:
            pass

        while True:
            self.prepare_response = 0
            self.ack_response = 0

            # if site is the leader:
            if self.leader is True:
                if learning is True:
                    leader_acc = {'accept': 'accept', 'accNum': 0, 'accVal': 'dummy', 'update': broadcast_event}
                else:
                    leader_acc = {'accept': 'accept', 'accNum': 0, 'accVal': broadcast_event}
                print('proposer: send accept(0, v) to all acceptor as a leader')
                print('v is ' + str(leader_acc['accVal']))
                print()
                self.broadcast(leader_acc)
                time.sleep(0.1)
                print('proposer: receive %d ack to accept(0, v)' % self.ack_response)
                print()
                if self.ack_response > 2:
                    for a_value in self.ack_value:
                        if a_value is None:
                            continue
                        else:
                            commit = {'commit': 'commit', 'value': a_value}
                            break
                    print('proposer: sent commit(v) to all acceptors')
                    print('v is ' + str(commit['value']))
                    print()
                    self.broadcast(commit)
                    self.response_value = []
                    self.ack_value = []
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
                if self.prepare_response < 3:
                    count += 1
                    m += 1
                    if count > 3:
                        give_up = {'give_up': 'give_up'}
                        self.broadcast(give_up)
                        if learning is True:
                            print('fail to update this log !')
                            self.is_updated = False
                        break
                    time.sleep(30)

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
                    if learning is True:
                        accept = {'accept': 'accept', 'accNum': prepareNum, 'accVal': 'dummy', 'update': broadcast_event}
                    else:
                        accept = {'accept': 'accept', 'accNum': prepareNum, 'accVal': broadcast_event}
                else:
                    accept = {'accept': 'accept', 'accNum': prepareNum, 'accVal': max_accVal}

                print('proposer: sent accept(n, v) to all acceptors')
                print('v is ' + str(accept['accVal']))
                print()
                self.broadcast(accept)
                time.sleep(0.1)
                print('proposer: received %d ack to accept(n, v)' % self.ack_response)
                print()
                # send commit to acceptors who will write the log
                if self.ack_response > 2:
                    for a_value in self.ack_value:
                        if a_value is None:
                            if self.ack_value.index(a_value) == len(self.ack_value) - 1:
                                commit = {'commit': 'commit', 'value': a_value}
                            else:
                                continue
                        else:
                            commit = {'commit': 'commit', 'value': a_value}
                            break
                    print('proposer: sent commit(v) to all acceptors')
                    print('v is ' + str(commit['value']))
                    print()
                    self.broadcast(commit)
                    self.response_value = []
                    self.ack_value = []
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

    def store_variable(self, Paxos_V):
        store_paxos = open('variable.pkl', 'wb')
        pickle.dump(Paxos_V, store_paxos)
        store_paxos.close()

    def learn(self):

        # ask for max log id of other sites
        learn_event = {'name': self.id_self, 'ask': 'ask'}
        self.broadcast(learn_event)
        time.sleep(0.2)
        try:
            max_logid = max(self.logid)
            gap = max_logid - len(self.log)
        except ValueError:
            gap = 0
            print('timeout to get max log id from other sites!')
            print('fail to update the log!')
            print()
        update = len(self.log) + 1
        while gap > 0:
            self.synod_broadcast(update, True)
            time.sleep(1.2)
            if self.is_updated is True:
                gap -= 1
                update += 1
            else:
                pass
        print('log has been updated')
        print()
        self.logid = []


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
    print('We have ' + str(cinstance.failed_site + 1) + ' downed site(s).')

    while True:
        time.sleep(0.5)
        print('What do you want to do?')
        choice = input('')

        if choice == 'tweet':
            print('Say something: ', end='')
            tweet = input('')
            t_time = datetime.datetime.utcnow()
            t_event = {'name': cinstance.id_self, 'op': 'tweet: %s' % tweet,
                       'tweet': tweet, 'time': str(t_time), 'logid': len(cinstance.log) + 1}
            cinstance.synod_broadcast(t_event)

        if choice == 'block':
            print('Who do you want to block?')
            block_id = input('')
            b_time = datetime.datetime.utcnow()
            b_event = {'name': cinstance.id_self, 'op': 'block %s' % block_id,
                       'block': block_id, 'time': str(b_time), 'logid': len(cinstance.log) + 1}
            cinstance.synod_broadcast(b_event)

        if choice == 'unblock':
            print('Who do you want to unblock?')
            unblock_id = input('')
            u_time = datetime.datetime.utcnow()
            u_event = {'name': cinstance.id_self, 'op': 'unblock %s' % unblock_id,
                       'unblock': unblock_id, 'time': str(u_time), 'logid': len(cinstance.log) + 1}
            cinstance.synod_broadcast(u_event)

        if choice == 'view':

            # update in-memory data structure of block and unblock information
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
                        timelines = tweet['name'] + ' tweets: "' + tweet['tweet'] + '" at ' + str(tweet['time'])
                        print(timelines)
            cinstance.log.reverse()

        if choice == 'log':
            for logs in cinstance.log:
                if logs is None:
                    print(logs)
                else:
                    print(logs['name'], logs['op'], logs['time'])

        if choice == 'original':
            for original in cinstance.log:
                print(original)

        if choice == 'learn':
            cinstance.learn()

        else:
            continue


