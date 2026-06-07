import hashlib
import re
from datetime import datetime

# --- Same secret keys as in Java ---
OPE_MULTIPLIER = 143.77
OPE_SHIFT = 8921.45

def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()[:16]

def _ope_encrypt(value: float) -> float:
    return (value * OPE_MULTIPLIER) + OPE_SHIFT

def encrypt_subscription(sub_dict: dict) -> dict:
    encrypted = {}
    for field, condition in sub_dict.items():
        if not isinstance(condition, tuple):
            encrypted[field] = condition
            continue

        op, value = condition

        if isinstance(value, str):
            # Encrypt text with hash
            encrypted[field] = (op, _hash_text(value))
        elif isinstance(value, (int, float)):
            # Encrypt numbers with OPE
            encrypted[field] = (op, _ope_encrypt(float(value)))
        else:
            encrypted[field] = (op, value)

    return encrypted

def parse_java_publication(pub_str):
    pub_data = {}
    pub_str = pub_str.strip().strip('{}')
    matches = re.findall(r'\(\s*([^,]+?)\s*,\s*(".*?"|[^)]*?)\s*\)', pub_str)
    for field, val in matches:
        field = field.strip()
        val = val.strip()
        if val.startswith('"') and val.endswith('"'):
            pub_data[field] = val[1:-1]
        else:
            try:
                pub_data[field] = float(val) if '.' in val else int(val)
            except ValueError:
                pub_data[field] = val
    return pub_data

def parse_java_subscription(sub_str):
    sub_data = {}
    sub_str = sub_str.strip().strip('{}')
    matches = re.findall(
        r'\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*(".*?"|[^)]*?)\s*\)',
        sub_str
    )
    for field, op, val in matches:
        field = field.strip()
        op = op.strip()
        val = val.strip()
        if val.startswith('"') and val.endswith('"'):
            parsed_val = val[1:-1]
        else:
            try:
                parsed_val = float(val) if '.' in val else int(val)
            except ValueError:
                parsed_val = val
        sub_data[field] = (op, parsed_val)
    return sub_data

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        return None