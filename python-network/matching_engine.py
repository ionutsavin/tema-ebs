from typing import Dict
from datetime import datetime


def _try_parse_date(val):
    try:
        return datetime.strptime(val, "%d.%m.%Y")
    except (ValueError, TypeError):
        return None


class MatchingEngine:

    def matches(self, publication: Dict, subscription: Dict) -> bool:
        for field, condition in subscription.items():
            if field not in publication:
                return False

            pub_value = publication[field]

            if isinstance(condition, tuple) and len(condition) == 2:
                operator, sub_value = condition

                if isinstance(pub_value, str) and isinstance(sub_value, str):
                    pub_date = _try_parse_date(pub_value)
                    sub_date = _try_parse_date(sub_value)
                    if pub_date and sub_date:
                        pub_value = pub_date
                        sub_value = sub_date

                elif isinstance(pub_value, (int, float)) and isinstance(sub_value, str):
                    try:
                        sub_value = float(sub_value)
                    except ValueError:
                        pass

                elif isinstance(pub_value, str) and isinstance(sub_value, (int, float)):
                    try:
                        pub_value = float(pub_value)
                    except ValueError:
                        pass

                try:
                    if operator == "=":
                        if isinstance(pub_value, (int, float)) and isinstance(sub_value, (int, float)):
                            if abs(pub_value - sub_value) > 1e-7:
                                return False
                        elif pub_value != sub_value:
                            return False
                    elif operator == ">":
                        if not (pub_value > sub_value):
                            return False
                    elif operator == "<":
                        if not (pub_value < sub_value):
                            return False
                    elif operator == ">=":
                        if not (pub_value >= sub_value):
                            return False
                    elif operator == "<=":
                        if not (pub_value <= sub_value):
                            return False
                    elif operator == "!=":
                        if pub_value == sub_value:
                            return False
                except TypeError:
                    return False
            else:
                if pub_value != condition:
                    return False

        return True