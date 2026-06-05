"""
Evaluator pentru performanța sistemului
Măsoară: numărul de publicații livrate, latența medie, rata de matching
"""

import time
import threading
import statistics
from typing import List
from data_generator import DataGenerator
from broker import BrokerNetwork
from subscriber import Subscriber
from publisher import Publisher
from matching_engine import MatchingEngine


class SystemEvaluator:
    """Evaluator pentru performanța sistemului"""

    def __init__(self):
        self.generator = DataGenerator()
        self.stats = {
            'publications_delivered': 0,
            'latencies': [],
            'matching_rate_100': 0,
            'matching_rate_25': 0,
            'total_publications': 0,
            'subscriptions_registered': 0
        }
        self.lock = threading.Lock()

    def run_evaluation(self):
        """Rulează evaluarea completă a sistemului"""
        print("\n" + "=" * 70)
        print(" SISTEM PUBLISH/SUBSCRIBE CU FILTRARE PE CONTINUT")
        print("=" * 70)

        # Test generator cu paralelizare
        self._test_generator_performance()

        # 1. Inițializare rețea brokeri
        print("\n[1/5] Initializare retea brokeri...")
        broker_network = BrokerNetwork(num_brokers=3)

        # 2. Inițializare subscriberi
        print("[2/5] Initializare subscriberi...")
        subscribers = []
        for i in range(3):
            sub = Subscriber(f"subscriber_{i}", broker_network.get_broker_addresses())
            subscribers.append(sub)

        # 3. Înregistrare 10000 subscripții
        print("[3/5] Inregistrare 10000 subscriptii...")
        self._register_subscriptions(subscribers, 10000)

        # 4. Test livrare publicații (3 minute)
        print("[4/5] Test livrare publicatii timp de 3 minute...")
        self._test_publication_delivery(broker_network)

        # 5. Test rată de matching
        print("[5/5] Test rata de matching...")
        self._test_matching_rates()

        # 6. Afișare rezultate finale
        self._print_results()

        # 7. Statistici brokeri
        self._print_broker_stats(broker_network)

        broker_network.stop_all()

    def _test_generator_performance(self):
        """Testează performanța generatorului cu paralelizare"""
        print("\n--- TEST GENERATOR CU PARALELIZARE ---")

        for threads in [1, 4]:
            start_time = time.time()
            pubs = self.generator.generate_publications_parallel(10000, threads)
            elapsed = time.time() - start_time

            print(f"  Thread-uri: {threads} | Timp: {elapsed:.3f}s | Rata: {10000 / elapsed:.0f} pub/sec")

    def _register_subscriptions(self, subscribers: List[Subscriber], count: int):
        """Înregistrează subscripții cu ponderi configurabile"""
        field_frequencies = {
            'company': 0.9,
            'value': 0.7,
            'drop': 0.5,
            'variation': 0.6
        }

        start_time = time.time()
        per_subscriber = count // len(subscribers)

        for sub in subscribers:
            for _ in range(per_subscriber):
                sub_data = self.generator.generate_subscription_with_weights(
                    field_frequencies,
                    equality_frequency=1.0
                )
                sub.subscribe(sub_data)
                with self.lock:
                    self.stats['subscriptions_registered'] += 1

        elapsed = time.time() - start_time
        print(f"  Inregistrate {count} subscriptii in {elapsed:.2f} secunde")
        print(f"  Rata: {count / elapsed:.0f} sub/sec")

    def _test_publication_delivery(self, broker_network: BrokerNetwork):
        """livrarea publicațiilor timp de 3 minute"""
        publishers = [
            Publisher("publisher_1", broker_network.get_broker_addresses()),
            Publisher("publisher_2", broker_network.get_broker_addresses())
        ]

        start_time = time.time()
        duration = 180  # 3 minute

        def publish_loop(publisher: Publisher, stop_flag: threading.Event):
            while not stop_flag.is_set():
                pub = self.generator.generate_publication()
                success, latency = publisher.publish(pub)

                with self.lock:
                    if success:
                        self.stats['publications_delivered'] += 1
                        if latency > 0:
                            self.stats['latencies'].append(latency)
                    self.stats['total_publications'] += 1

                time.sleep(0.05)

        stop_flag = threading.Event()
        threads = []

        for pub in publishers:
            t = threading.Thread(target=publish_loop, args=(pub, stop_flag))
            t.start()
            threads.append(t)

        time.sleep(duration)
        stop_flag.set()

        for t in threads:
            t.join(timeout=2)

        print(f"  Publicatii trimise: {self.stats['total_publications']}")
        print(f"  Publicatii procesate cu succes: {self.stats['publications_delivered']}")

    def _test_matching_rates(self):
        """rata de matching pentru 100% și 25% operator '='"""
        engine = MatchingEngine()
        test_count = 5000
        field_freq = {'company': 1.0}

        # Cazul 1: 100% operator '='
        matches_100 = 0
        for _ in range(test_count):
            pub = self.generator.generate_publication()
            sub = self.generator.generate_subscription_with_weights(field_freq, equality_frequency=1.0)
            if engine.matches(pub, sub):
                matches_100 += 1

        # Cazul 2: 25% operator '='
        matches_25 = 0
        for _ in range(test_count):
            pub = self.generator.generate_publication()
            sub = self.generator.generate_subscription_with_weights(field_freq, equality_frequency=0.25)
            if engine.matches(pub, sub):
                matches_25 += 1

        rate_100 = matches_100 / test_count
        rate_25 = matches_25 / test_count

        self.stats['matching_rate_100'] = rate_100
        self.stats['matching_rate_25'] = rate_25

        print(f"  100% operator '=': {rate_100:.2%}")
        print(f"  25% operator '=': {rate_25:.2%}")
        print(f"  Raport: {rate_100 / rate_25:.2f}x mai multe potriviri")

    def _print_results(self):
        print("\n" + "=" * 70)
        print(" REZULTATE FINALE - RAPORT DE EVALUARE")
        print("=" * 70)

        # a) Numărul de publicații livrate
        print(f"\na) Publicatii livrate in 3 minute: {self.stats['publications_delivered']}")
        print(f"   (din {self.stats['total_publications']} publicatii trimise)")

        # b) Latența medie
        if self.stats['latencies'] and len(self.stats['latencies']) > 0:
            avg_latency = statistics.mean(self.stats['latencies'])
            median_latency = statistics.median(self.stats['latencies'])

            print(f"\nb) Latenta medie de livrare:")
            print(f"   Medie: {avg_latency:.2f} ms")
            print(f"   Mediana: {median_latency:.2f} ms")
            print(f"   Minima: {min(self.stats['latencies']):.2f} ms")
            print(f"   Maxima: {max(self.stats['latencies']):.2f} ms")

        # c) Rata de matching
        print(f"\nc) Rata de matching:")
        print(f"   100% operator '=' pe company: {self.stats['matching_rate_100']:.2%}")
        print(f"   25% operator '=' pe company: {self.stats['matching_rate_25']:.2%}")

    def _print_broker_stats(self, broker_network: BrokerNetwork):
        print("\n--- STATISTICI PER BROKER ---")
        stats = broker_network.get_stats()
        total_subs = sum(s['subscriptions'] for s in stats)

        for stat in stats:
            pct = (stat['subscriptions'] / total_subs * 100) if total_subs > 0 else 0
            print(
                f"  {stat['broker_id']}: {stat['subscriptions']} subscriptii ({pct:.1f}%), {stat['messages_processed']} notificari")