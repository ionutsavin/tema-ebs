"""
Consistent Hashing for distributed subscription routing across brokers
"""

import hashlib
from typing import List


class ConsistentHashRing:
    """Consistent hash ring for distributing subscriptions"""

    def __init__(self, nodes: List[str], virtual_nodes: int = 150):
        self.nodes = nodes.copy()
        self.virtual_nodes = virtual_nodes
        self.ring = {}
        self.sorted_keys = []
        self._build_ring()

    def _hash(self, key: str) -> int:
        """Compute MD5 hash for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def _build_ring(self):
        """Build ring with virtual nodes for even distribution"""
        self.ring.clear()
        for node in self.nodes:
            for i in range(self.virtual_nodes):
                virtual_key = f"{node}:{i}"
                hash_val = self._hash(virtual_key)
                self.ring[hash_val] = node
        self.sorted_keys = sorted(self.ring.keys())

    def get_node(self, key: str) -> str:
        """Get the node responsible for a given key"""
        if not self.ring:
            return None

        hash_val = self._hash(key)

        # Find first node with hash >= hash_val
        for key_hash in self.sorted_keys:
            if key_hash >= hash_val:
                return self.ring[key_hash]

        return self.ring[self.sorted_keys[0]]