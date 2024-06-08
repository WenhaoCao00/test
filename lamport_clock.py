class LamportClock:
    def __init__(self):
        self.time = 0

    def increment(self):
        self.time += 1

    def receive_timestamp(self, timestamp):
        self.time = max(self.time, timestamp) + 1

    def get_time(self):
        return self.time
