import threading
from collections import defaultdict, deque
from identity import IdentityContext
from enum import Enum
import psycopg2
import psycopg2.extras
import json

class IsolationLevel(Enum):
    READ_UNCOMMITTED = 1
    READ_COMMITTED = 2
    REPEATABLE_READ = 3
    SERIALIZABLE = 4

class VersionedData:
    def __init__(self, value, version, committed=False, deleted=False):
        self.value = value
        self.version = version
        self.committed = committed
        self.deleted = deleted

    def to_dict(self):
        return {
            'value': self.value,
            'version': self.version,
            'committed': self.committed,
            'deleted': self.deleted
        }

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

class Atomix:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = psycopg2.connect(**db_config)
        self.count = 0
        self.global_version = 0
        self._lock = threading.Lock()
        self._transactions = defaultdict(lambda: defaultdict(dict))
        self._stores = defaultdict(deque)
        self.transaction_reads = defaultdict(dict)
        self.default_isolation_level = IsolationLevel.READ_COMMITTED
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS atomix_data (
                    identity_id TEXT,
                    key TEXT,
                    versions JSONB,
                    PRIMARY KEY (identity_id, key)
                )
            """)
        self.conn.commit()

    def initiate(self, identity):
        return IdentityContext(self, identity.id)

    def commit(self, identity_id):
        with self._lock:
            with self.conn.cursor() as cur:
                for key, value in self._transactions[identity_id].items():
                    if isinstance(value, VersionedData):
                        value.committed = True
                        cur.execute("""
                            INSERT INTO atomix_data (identity_id, key, versions)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (identity_id, key) DO UPDATE
                            SET versions = atomix_data.versions || %s::jsonb
                        """, (identity_id, key, json.dumps([value.to_dict()]), json.dumps(value.to_dict())))
            self.conn.commit()
            self._transactions[identity_id].clear()

    def set_isolation_level(self, level):
        self.isolation_level = level

    def abort(self, identity_id):
        self._transactions[identity_id].clear()

    def read(self, identity_id, key, transaction_version, isolation_level):
        with self._lock:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT versions FROM atomix_data WHERE identity_id = %s AND key = %s", (identity_id, key))
                result = cur.fetchone()
                
                if result is None:
                    return None
                
                versions = [VersionedData.from_dict(v) for v in result['versions']]

                if isolation_level == IsolationLevel.READ_UNCOMMITTED:
                    if versions:
                        latest_version = versions[-1]
                        if not latest_version.deleted:
                            return latest_version.value
                    return None

                elif isolation_level == IsolationLevel.READ_COMMITTED:
                    for version in reversed(versions):
                        if version.committed and version.version <= self.global_version and not version.deleted:
                            return version.value
                    return None

                elif isolation_level == IsolationLevel.REPEATABLE_READ:
                    if key in self.transaction_reads[identity_id]:
                        return self.transaction_reads[identity_id][key]
                    
                    for version in reversed(versions):
                        if version.committed and version.version <= transaction_version and not version.deleted:
                            self.transaction_reads[identity_id][key] = version.value
                            return version.value
                    return None

                elif isolation_level == IsolationLevel.SERIALIZABLE:
                    for version in reversed(versions):
                        if version.committed and version.version <= transaction_version and not version.deleted:
                            if key not in self.transaction_reads[identity_id]:
                                self.transaction_reads[identity_id][key] = version.value
                            return self.transaction_reads[identity_id][key]
                    return None

    def write(self, identity_id, key, value, write_version):
        with self._lock:
            new_version = VersionedData(value, write_version, committed=False)
            self._transactions[identity_id][key] = new_version

    def delete(self, identity_id, key):
        with self._lock:
            delete_version = VersionedData(None, self.global_version, committed=False, deleted=True)
            self._transactions[identity_id][key] = delete_version

    def store(self, identity_id, key, value):
        pair = [key, value]
        self._stores[identity_id].append(pair) 

    def offload_store(self, identity_id):
        while self._stores[identity_id]: 
            pair = self._stores[identity_id].pop()
            self._transactions[identity_id][pair[0]] = pair[1]

    def garbage_collector(self, identity_id, max_versions=10):
        with self._lock:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT key, versions FROM atomix_data WHERE identity_id = %s", (identity_id,))
                results = cur.fetchall()
            
                if not results:
                    return None
            
                updates = []
                for row in results:
                    key = row['key']
                    versions = [VersionedData.from_dict(v) for v in row['versions']]
                    if len(versions) > max_versions:
                        versions = versions[-max_versions:]  # Keep only the last max_versions
                        updates.append((key, [v.to_dict() for v in versions]))
            
                if updates:
                    update_query = """
                    UPDATE atomix_data 
                    SET versions = %s 
                    WHERE identity_id = %s AND key = %s
                    """
                    for key, collected_versions in updates:
                        cur.execute(update_query, (json.dumps(collected_versions), identity_id, key))
                
                    self.conn.commit()
            
                return len(updates)  # Return the number of keys that were garbage collected
                    
    def register(self, identity_id):
        with self._lock:
            if identity_id not in self._transactions:
                self._transactions[identity_id] = defaultdict(dict)
                self._stores[identity_id] = deque()
                self.count += 1

    def __del__(self):
        if self.conn:
            self.conn.close()