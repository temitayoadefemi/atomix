import threading
from collections import defaultdict, deque
from identity import IdentityContext
from enum import Enum

class IsolationLevel(Enum):
    READ_UNCOMMITTED = 1
    READ_COMMITTED = 2
    REPEATABLE_READ = 3
    SERIALIZABLE = 4

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
    
    def set_isolation_level(self, level):
        self.isolation_level = level

    def read(self, identity_id, key):
        if self.isolation_level == IsolationLevel.READ_UNCOMMITTED:
            return self._transactions[identity_id].get(key, self.data.get(key))
        
        elif self.isolation_level == IsolationLevel.READ_COMMITTED:
            with self._lock:
                return self.data.get(key)
        
        elif self.isolation_level == IsolationLevel.REPEATABLE_READ:
            if key not in self._transactions[identity_id]:
                with self._lock:
                    self._transactions[identity_id][key] = self.data.get(key)
            return self._transactions[identity_id][key]
        
        elif self.isolation_level == IsolationLevel.SERIALIZABLE:
            with self._lock:
                return self.data.get(key)
            
    def abort(self, identity_id):
        self._transactions[identity_id].clear()
    
    def read(self, identity_id, key, read_version):
        with self._lock:
            versions = self.version.history[key]
            for version in reversed(versions):
                if version.version <= read_version and not version.deleted:
                    return version.value
                return None
            
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

