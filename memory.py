import threading
from collections import defaultdict, deque
from identity import IdentityContext

class Atomix:
    def __init__(self):
        self.data = {}
        self.count = 0
        self._lock = threading.Lock()
        self._transactions = defaultdict(lambda: defaultdict(dict))
        self._stores = defaultdict(lambda: defaultdict(deque))

    def initiate(self, identity):
        return IdentityContext(self, identity.id)

    def commit(self, identity_id):
        with self._lock:
            self.data.update(self._transactions[identity_id])
            self._transactions[identity_id].clear()

    def abort(self, identity_id):
        self._transactions[identity_id].clear()
    
    def read(self, identity_id, key):
        if key in self._transactions[identity_id]:
            return self._transactions[identity_id][key]
        with self._lock:
            return self.data[key]  
    
    def write(self, identity_id, key, value):
        self._transactions[identity_id][key] = value

    def delete(self, identity_id, key):
        self._transactions[identity_id][key] = None

    def store(self, identity_id, key, value):
        pair = [key, value]
        self._stores[identity_id].append(pair) 

    def offload_store(self, identity_id):
        while self._stores[identity_id]: 
            pair = self._stores[identity_id].pop()
            self._transactions[identity_id][pair[0]] = pair[1]

    def register(self, identity):
        with self._lock:
            if identity.id not in self._transactions:
                self._transactions[identity.id] = defaultdict(dict)
                self._stores[identity.id] = deque()
                self.count += 1

