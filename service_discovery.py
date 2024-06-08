import socket
import threading
import time

class ServiceDiscovery:
    def __init__(self):
        self.local_ip = socket.gethostbyname(socket.gethostname())
        self.broadcast_ip = '255.255.255.255'
        self.port = 5000
        self.server_addresses = set()
        self.lock = threading.Lock()

    def send_broadcast(self):
        message = b'SERVICE_DISCOVERY'
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind((self.local_ip, 0))
        while True:
            sock.sendto(message, (self.broadcast_ip, self.port))
            time.sleep(5)

    def listen_for_broadcast(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', self.port))
        while True:
            data, addr = sock.recvfrom(1024)
            if data == b'SERVICE_DISCOVERY' and self.is_valid_ip(addr[0]) and addr[0] not in self.server_addresses:
                with self.lock:
                    self.server_addresses.add(addr[0])
                print(f"Discovered server: {addr[0]}")
                self.notify_existing_servers(addr[0])

    def notify_existing_servers(self, new_server_ip):
        notification_message = f'NEW_SERVER:{new_server_ip}'.encode()
        for server_ip in self.server_addresses:
            if server_ip != new_server_ip:
                self.send_notification(server_ip, notification_message)

    def send_notification(self, server_ip, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(message, (server_ip, self.port))
            print(f"Notification sent to {server_ip}: {message.decode()}")
        except Exception as e:
            print(f"Error sending notification to {server_ip}: {e}")
        finally:
            sock.close()

    def start(self):
        threading.Thread(target=self.send_broadcast, daemon=True).start()
        threading.Thread(target=self.listen_for_broadcast, daemon=True).start()

    def get_servers(self):
        with self.lock:
            return list(self.server_addresses)
    
    def find_leader(self):
        servers = self.get_servers()
        return servers[0] if servers else None

    def is_valid_ip(self, ip):
        # Add your own validation logic if needed
        return True
