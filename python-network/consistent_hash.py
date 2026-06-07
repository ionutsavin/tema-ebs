"""
Consistent Hashing pentru rutarea distribuită a subscripțiilor între brokeri
"""

import hashlib
from typing import List


class ConsistentHashRing:
    """Inel de hash consistent pentru distribuția subscripțiilor"""

    def __init__(self, nodes: List[str], virtual_nodes: int = 150):
        self.nodes = nodes.copy()
        self.virtual_nodes = virtual_nodes
        self.ring = {}
        self.sorted_keys = []
        self._build_ring()

    def _hash(self, key: str) -> int:
        """Calculează hash MD5 pentru o cheie"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def _build_ring(self):
        """inelul cu noduri virtuale pentru distribuție uniformă"""
        self.ring.clear()
        for node in self.nodes:
            for i in range(self.virtual_nodes):
                virtual_key = f"{node}:{i}"
                hash_val = self._hash(virtual_key)
                self.ring[hash_val] = node
        self.sorted_keys = sorted(self.ring.keys())

    def get_node(self, key: str) -> str:
        """nodul responsabil pentru o cheie dată"""
        if not self.ring:
            return None

        hash_val = self._hash(key)

        # Găsește primul nod cu hash >= hash_val
        for key_hash in self.sorted_keys:
            if key_hash >= hash_val:
                return self.ring[key_hash]

        return self.ring[self.sorted_keys[0]]