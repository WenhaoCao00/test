import socket
import threading
import time
import uuid
import json
from service_discovery import ServiceDiscovery
from lamport_clock import LamportClock

class ChatClient:
    def __init__(self):
        self.service_discovery = ServiceDiscovery()
        self.clock = LamportClock()
        self.server_port = 10001
        self.leader_address = None
        self.client_socket = None
        self.client_name = None
        self.message_queue = []
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 5  # seconds

    def receive_messages(self):
        while True:
            try:
                data, server = self.client_socket.recvfrom(1024)
                message = json.loads(data.decode())
                self.clock.receive_timestamp(message['timestamp'])
                if message['type'] == 'CHAT':
                    self.message_queue.append((message['timestamp'], message['text']))
                    self.message_queue.sort()  # 保证按时间戳排序
                    self.display_messages()
                    self.send_ack(message['id'], server)
                elif message['type'] == 'ACK':
                    self.handle_ack(message['id'])
                elif message['type'] == 'HEARTBEAT':
                    self.last_heartbeat = time.time()
            except OSError as e:
                print(f"\rError receiving data: {e}\n", end='', flush=True)
                break

    def display_messages(self):
        for timestamp, text in self.message_queue:
            print(f"\r{text}\n{self.client_name}: ", end='', flush=True)
        self.message_queue.clear()

    def send_ack(self, message_id, server):
        ack_message = {
            'type': 'ACK',
            'id': message_id,
            'timestamp': self.clock.get_time()
        }
        self.client_socket.sendto(json.dumps(ack_message).encode(), server)

    def handle_ack(self, message_id):
        # 处理ACK消息的逻辑
        pass

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

    def set_client_name(self):
        self.client_name = f'({self.get_local_ip()}:{self.client_socket.getsockname()[1]})'

    def send_messages(self):
        try:
            while True:
                message = input(f"{self.client_name}: ")
                if message.strip().lower() == "exit":
                    print("Exiting...")
                    break
                self.clock.increment()
                message_id = str(uuid.uuid4())
                full_message = {
                    'type': 'CHAT',
                    'id': message_id,
                    'timestamp': self.clock.get_time(),
                    'text': f'{self.client_name}: {message}'
                }
                self.client_socket.sendto(json.dumps(full_message).encode(), (self.leader_address, self.server_port))
        except KeyboardInterrupt:
            print("Client is closing.")
        finally:
            self.client_socket.close()

    def find_leader(self, server_addresses):
        self.create_socket()
        for server_address in server_addresses:
            try:
                self.client_socket.sendto(b'IS_LEADER', (server_address, self.server_port))
                data, server = self.client_socket.recvfrom(1024)
                message = data.decode()
                if message == 'LEADER':
                    return server_address
            except ConnectionResetError:
                print(f"Connection to {server_address} was reset. Trying next server...")
                continue
        return None

    def discover_servers(self):
        self.service_discovery.start()
        time.sleep(5)
        return list(self.service_discovery.get_servers())

    def create_socket(self):
        if self.client_socket:
            self.client_socket.close()
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.client_socket.bind(('0.0.0.0', 0))

    def main(self):
        server_addresses = self.discover_servers()
        print("Discovered servers:", server_addresses)
    
        if not server_addresses:
            print("No servers discovered, exiting.")
            return
        
        self.leader_address = self.find_leader(server_addresses)
        if self.leader_address is None:
            print("No leader found, exiting.")
            return

        while True:
            self.create_socket()
            self.set_client_name()
            print(f'Client bound to port {self.client_socket.getsockname()[1]}')
            
            self.leader_address = self.find_leader(server_addresses)
            if self.leader_address is None:
                print("No leader found after reattempt, exiting.")
                return
            

            receiver_thread = threading.Thread(target=self.receive_messages, daemon=True)
            receiver_thread.start()

            self.send_messages()

            self.client_socket.close()
            print('Socket closed')
            break

if __name__ == '__main__':
    client = ChatClient()
    client.main()
