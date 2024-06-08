import socket
import threading
import time
import json
from lamport_clock import LamportClock
from service_discovery import ServiceDiscovery
from ring_election import initiate_election

class ChatServer:
    def __init__(self, is_leader=False):
        self.service_discovery = ServiceDiscovery()
        self.clock = LamportClock()
        self.is_leader = is_leader
        self.server_port = 10001
        self.clients = {}
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind(("", self.server_port))
        self.my_ip = self.get_local_ip()
        self.leader_ip = self.my_ip if is_leader else None
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 5  # seconds

    def get_local_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = socket.gethostbyname(socket.gethostname())
        finally:
            s.close()
        return ip

    def handle_client(self, data, client_address):
        message = json.loads(data.decode())
        self.clock.receive_timestamp(message['timestamp'])
        if message['type'] == 'CHAT':
            self.broadcast_message(message)
            self.send_ack(message['id'], client_address)
        elif message['type'] == 'ACK':
            self.handle_ack(message['id'], client_address)
        elif message['type'] == 'HEARTBEAT':
            self.last_heartbeat = time.time()

    def broadcast_message(self, message):
        for client_address in self.clients.values():
            self.server_socket.sendto(json.dumps(message).encode(), client_address)

    def send_ack(self, message_id, client_address):
        ack_message = {
            'type': 'ACK',
            'id': message_id,
            'timestamp': self.clock.get_time()
        }
        self.server_socket.sendto(json.dumps(ack_message).encode(), client_address)

    def handle_ack(self, message_id, client_address):
        # 处理ACK消息的逻辑
        pass

    def send_heartbeat(self):
        while self.is_leader:
            time.sleep(self.heartbeat_interval)
            heartbeat_message = {
                'type': 'HEARTBEAT',
                'timestamp': self.clock.get_time()
            }
            self.broadcast_message(heartbeat_message)

    def start_server(self):
        print("Server started...")
        threading.Thread(target=self.run_election, daemon=True).start()
        if self.is_leader:
            threading.Thread(target=self.send_heartbeat, daemon=True).start()
        while True:
            data, client_address = self.server_socket.recvfrom(1024)
            client_addr = f'{client_address[0]}:{client_address[1]}'
            if client_addr not in self.clients:
                self.clients[client_addr] = client_address
                print(f'New client connected: {client_addr}')
                self.run_election()
            self.handle_client(data, client_address)

    def run_election(self):
        print("Initiating election...")
        discovered_servers = self.service_discovery.get_servers()
        all_servers = discovered_servers + [self.my_ip]
        self.leader_ip = initiate_election(all_servers, self.my_ip)
        if self.leader_ip == self.my_ip:
            self.is_leader = True
            print("This server is now the leader.")
            threading.Thread(target=self.send_heartbeat, daemon=True).start()
        else:
            self.is_leader = False
            print(f"The leader is {self.leader_ip}.")

    def check_leader_heartbeat(self):
        while True:
            time.sleep(self.heartbeat_interval)
            if time.time() - self.last_heartbeat > self.heartbeat_interval * 2:
                print("Leader is down, initiating re-election.")
                self.run_election()

if __name__ == "__main__":
    is_leader = input("Is this server the leader? (yes/no): ").lower() == 'yes'
    server = ChatServer(is_leader)
    threading.Thread(target=server.check_leader_heartbeat, daemon=True).start()
    server.start_server()
