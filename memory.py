import threading
from collections import defaultdict, deque
from identity import IdentityContext
from enum import Enum
from version import VersionedData

class IsolationLevel(Enum):
    READ_UNCOMMITTED = 1
    READ_COMMITTED = 2
    REPEATABLE_READ = 3
    SERIALIZABLE = 4

class Atomix:
    def __init__(self):
        self.data = {}
        self.count = 0
        self.version_history = defaultdict(lambda: defaultdict(list))
        self.global_version = 0
        self._lock = threading.Lock()
        self._transactions = defaultdict(lambda: defaultdict(dict))
        self._stores = defaultdict(deque)
        self.transaction_reads = defaultdict(dict)
        self.default_isolation_level = IsolationLevel.READ_COMMITTED

    def initiate(self, identity):
        return IdentityContext(self, identity.id)

    def commit(self, identity_id):
        with self._lock:
            self.data.update(self._transactions[identity_id])
            for key, value in self._transactions[identity_id].items():
                if isinstance(value, VersionedData):
                    value.committed = True
            self._transactions[identity_id].clear()
    
    def set_isolation_level(self, level):
        self.isolation_level = level

    def abort(self, identity_id):
        self._transactions[identity_id].clear()

    def read(self, identity_id, key, transaction_version, isolation_level):
        with self._lock:
            if isolation_level == IsolationLevel.READ_UNCOMMITTED:
                versions = self.version_history[identity_id][key]
                if versions:
                    latest_version = versions[-1]
                    if not latest_version.deleted:
                        return latest_version.value
                return None

            elif isolation_level == IsolationLevel.READ_COMMITTED:
                versions = self.version_history[identity_id][key]
                for version in reversed(versions):
                    if version.committed and version.version <= self.global_version and not version.deleted:
                        return version.value
                return None

            elif isolation_level == IsolationLevel.REPEATABLE_READ:
                if key in self.transaction_reads[identity_id]:
                    return self.transaction_reads[identity_id][key]
                
                versions = self.version_history[identity_id][key]
                for version in reversed(versions):
                    if version.committed and version.version <= transaction_version and not version.deleted:
                        self.transaction_reads[identity_id][key] = version.value
                        return version.value
                return None

            elif isolation_level == IsolationLevel.SERIALIZABLE:
                versions = self.version_history[identity_id][key]
                for version in reversed(versions):
                    if version.committed and version.version <= transaction_version and not version.deleted:
                        if key not in self.transaction_reads[identity_id]:
                            self.transaction_reads[identity_id][key] = version.value
                        return self.transaction_reads[identity_id][key]
                return None
            
    def write(self, identity_id, key, value, write_version):
        with self._lock:
            new_version = VersionedData(value, write_version, committed=False)
            self.version_history[identity_id][key].append(new_version)
            self._transactions[identity_id][key] = new_version

    def delete(self, identity_id, key):
        with self._lock:
            delete_version = VersionedData(None, self.global_version, committed=False, deleted=True)
            self.version_history[identity_id][key].append(delete_version)
            self._transactions[identity_id][key] = delete_version

    def store(self, identity_id, key, value):
        pair = [key, value]
        self._stores[identity_id].append(pair) 

    def offload_store(self, identity_id):
        while self._stores[identity_id]: 
            pair = self._stores[identity_id].pop()
            self._transactions[identity_id][pair[0]] = pair[1]

    def register(self, identity_id):
        with self._lock:
            if identity_id not in self._transactions:
                self._transactions[identity_id] = defaultdict(dict)
                self._stores[identity_id] = deque()
                self.count += 1