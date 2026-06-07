import base64
import re
from datetime import datetime
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding

# --- Same secret keys as in Java ---
OPE_MULTIPLIER = 143.77
OPE_SHIFT = 8921.45

AES_KEY = b'EBSp@ss!2024!!a!'

def _encrypt_text(value: str) -> str:
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(value.encode('utf-8')) + padder.finalize()
    cipher = Cipher(algorithms.AES(AES_KEY), modes.ECB())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(padded_data) + encryptor.finalize()
    return base64.b64encode(ciphertext).decode()

def _decrypt_text(value: str) -> str:
    ciphertext = base64.b64decode(value)
    cipher = Cipher(algorithms.AES(AES_KEY), modes.ECB())
    decryptor = cipher.decryptor()
    padded_data = decryptor.update(ciphertext) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    plaintext = unpadder.update(padded_data) + unpadder.finalize()
    return plaintext.decode('utf-8')

def _ope_encrypt(value: float) -> float:
    return (value * OPE_MULTIPLIER) + OPE_SHIFT

def _ope_decrypt(value: float) -> float:
    return (value - OPE_SHIFT) / OPE_MULTIPLIER


def encrypt_subscription(sub_dict: dict) -> dict:
    encrypted = {}
    for field, condition in sub_dict.items():
        if not isinstance(condition, tuple):
            encrypted[field] = condition
            continue

        op, value = condition

        if isinstance(value, str):
            encrypted[field] = (op, _encrypt_text(value))
        elif isinstance(value, (int, float)):
            encrypted[field] = (op, _ope_encrypt(float(value)))
        else:
            encrypted[field] = (op, value)

    return encrypted


def decrypt_publication(encrypted_pub: dict) -> dict:
    plain = {}
    for field, value in encrypted_pub.items():
        if field == '_ts':
            plain[field] = value
        elif isinstance(value, (int, float)):
            plain[field] = round(_ope_decrypt(float(value)), 2)
        elif isinstance(value, str):
            try:
                plain[field] = _decrypt_text(value)
            except Exception:
                plain[field] = value
        else:
            plain[field] = value
    return plain


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