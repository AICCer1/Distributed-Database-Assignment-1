import hashlib
import json
import csv
import datetime
import bisect

# Constant definitions
MANAGER_QUEUE = 'manager_queue'
CLIENT_QUEUE = 'client_queue'

# Consistent hashing function
def hash_key(key, total_slots=1024):
    hash_obj = hashlib.md5(str(key).encode())
    return int(hash_obj.hexdigest(), 16) % total_slots

def get_keeper_id(key, num_keepers):

    hash_value = hash_key(key)
    return hash_value % num_keepers

def create_message(msg_type, data=None):
    return json.dumps({
        'type': msg_type,
        'data': data
    })

def parse_message(message):
    try:
        if isinstance(message, bytes):
            message = message.decode('utf-8')
        return json.loads(message)
    except Exception as e:
        print(f"Error parsing message: {str(e)}")
        print(f"Message content: {message}")
        return {}

def parse_date(date_str):
    try:
        # Try to parse DD-MM-YYYY format
        return datetime.datetime.strptime(date_str, '%d-%m-%Y').strftime('%Y-%m-%d')
    except ValueError:
        try:
            # Try to parse YYYY-MM-DD format (in case the data is already in this format)
            datetime.datetime.strptime(date_str, '%Y-%m-%d')
            return date_str  # If already in YYYY-MM-DD format, return directly
        except ValueError:
            return None

def format_date(date_str):
    try:
        return datetime.datetime.strptime(date_str, '%Y-%m-%d').strftime('%d-%m-%Y')
    except ValueError:
        return date_str

class ConsistentHashRing:
    def __init__(self):
        self.ring = []
        self.nodes = {}

    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node):
        self.nodes[node] = self._hash(node)
        self.ring.append(self.nodes[node])
        self.ring.sort()

    def remove_node(self, node):
        if node in self.nodes:
            self.ring.remove(self.nodes[node])
            del self.nodes[node]

    def get_node(self, key):
        if not self.ring:
            return None
        key_hash = self._hash(key)
        idx = bisect.bisect(self.ring, key_hash) % len(self.ring)
        return list(self.nodes.keys())[idx]