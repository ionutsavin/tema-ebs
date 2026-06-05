"""
Generator de date pentru publicații și subscripții
Cu suport pentru generare paralelă folosind thread-uri
"""

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict


class DataGenerator:
    """cu suport pentru paralelizare"""

    COMPANIES = ["Google", "Microsoft", "Apple", "Amazon", "Meta", "Netflix", "Tesla", "IBM"]

    def __init__(self):
        self.stats_lock = threading.Lock()
        self.generation_stats = {"total": 0, "by_thread": {}}

    def generate_publication(self) -> Dict:
        company = random.choice(self.COMPANIES)
        return {
            'company': company,
            'value': round(random.uniform(50.0, 500.0), 2),
            'drop': round(random.uniform(0.0, 50.0), 2),
            'variation': round(random.uniform(-2.0, 2.0), 3),
            'date': f"{random.randint(1, 28)}.{random.randint(1, 12)}.2024",
            'timestamp': int(time.time() * 1000),
            'id': random.randint(1000, 9999)
        }

    def generate_publications_parallel(self, count: int, num_threads: int = 4) -> List[Dict]:
        """publicații în paralel folosind thread-uri"""
        publications = []

        def generate_batch(batch_size: int, thread_id: int) -> List[Dict]:
            batch = [self.generate_publication() for _ in range(batch_size)]
            with self.stats_lock:
                self.generation_stats["total"] += batch_size
                self.generation_stats["by_thread"][thread_id] = batch_size
            return batch

        # Distribuirea lucrului între thread-uri
        base_batch = count // num_threads
        remainder = count % num_threads
        batch_sizes = [base_batch + (1 if i < remainder else 0) for i in range(num_threads)]

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for i, batch_size in enumerate(batch_sizes):
                if batch_size > 0:
                    futures.append(executor.submit(generate_batch, batch_size, i))

            for future in as_completed(futures):
                publications.extend(future.result())

        return publications

    def generate_subscription_with_weights(self,
                                           field_frequencies: Dict[str, float],
                                           equality_frequency: float = 0.7) -> Dict:
        """
        Generează o subscripție cu frecvențe configurabile
        field_frequencies: dict cu frecvența fiecărui câmp
        equality_frequency: procentul de operatori '=' pe câmpul company
        """
        subscription = {}

        for field, freq in field_frequencies.items():
            if random.random() < freq:
                if field == "company":
                    if random.random() < equality_frequency:
                        op = "="
                    else:
                        op = random.choice([">", "<", ">=", "<="])
                    value = random.choice(self.COMPANIES)
                    subscription[field] = (op, value)

                elif field == "value":
                    op = random.choice(["", ">", "<", ">=", "<="])
                    value = round(random.uniform(50, 500), 2)
                    subscription[field] = (op, value)

                elif field == "drop":
                    op = random.choice(["", ">", "<", ">=", "<="])
                    value = round(random.uniform(0, 50), 2)
                    subscription[field] = (op, value)

                elif field == "variation":
                    op = random.choice(["", ">", "<", ">=", "<="])
                    value = round(random.uniform(-2, 2), 3)
                    subscription[field] = (op, value)

        return subscription