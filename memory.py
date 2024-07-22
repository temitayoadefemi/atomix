import threading
from collections import defaultdict, deque

class Atomix:
    def __init__(self):
        self.data = {}
        self._local = threading.local()
        self._lock = threading.Lock()

    def start_transaction(self):
        self._local.transaction = defaultdict(dict)

    def commit(self):
        with self._lock:
            self.data.update(self._local.transaction)
            self._local.transaction.clear()

    def abort(self):
        self._local.transaction.clear()
    
    def read(self, key):
        if key in self._local.transaction:
            return self._local.transaction[key]
        with self._lock:
            return self.data[key]  # Added return statement
    
    def write(self, key, value):
        self._local.transaction[key] = value

    def delete(self, key):
        self._local.transaction[key] = None
    
    def create_store(self):
        self._local.store = deque()

    def store(self, key, value):
        pair = [key, value]
        self._local.store.append(pair)  # Changed queue to store

    def offload_store(self):
        while self._local.store:  # Changed self.local to self._local
            pair = self._local.store.pop()
            self._local.transaction[pair[0]] = pair[1]