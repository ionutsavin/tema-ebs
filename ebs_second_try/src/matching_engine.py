"""
Motor de filtrare pe bază de conținut pentru publicații și subscripții
"""

from typing import Dict


class MatchingEngine:

    def matches(self, publication: Dict, subscription: Dict) -> bool:
        for field, condition in subscription.items():
            # Verifică dacă câmpul există în publicație
            if field not in publication:
                return False

            pub_value = publication[field]

            if isinstance(condition, tuple) and len(condition) == 2:
                operator, value = condition

                if isinstance(pub_value, (int, float)):
                    if isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                        value = float(value)

                if operator == "=":
                    if pub_value != value:
                        return False
                elif operator == ">":
                    if not (pub_value > value):
                        return False
                elif operator == "<":
                    if not (pub_value < value):
                        return False
                elif operator == ">=":
                    if not (pub_value >= value):
                        return False
                elif operator == "<=":
                    if not (pub_value <= value):
                        return False
            else:
                if pub_value != condition:
                    return False

        return True