import re
from datetime import datetime


def parse_java_publication(pub_str):
    """Converteste formatul Java {(camp,valoare);...} in dict Python"""
    pub_data = {}
    # Elimină acoladelele și split după ';'
    pub_str = pub_str.strip().strip('{}')

    # Regex robust: captează (field, value) unde value poate conține ghilimele
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
    """Converteste formatul Java {(camp,operator,valoare);...} in dict pentru MatchingEngine"""
    sub_data = {}
    sub_str = sub_str.strip().strip('{}')

    # Regex robust: al treilea grup acceptă și ghilimele
    matches = re.findall(
        r'\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*(".*?"|[^)]*?)\s*\)',
        sub_str
    )

    for field, op, val in matches:
        field = field.strip()
        op = op.strip()
        val = val.strip()
        if val.startswith('"') and val.endswith('"'):
            parsed_val = val[1:-1]  # rămâne string
        else:
            try:
                parsed_val = float(val) if '.' in val else int(val)
            except ValueError:
                parsed_val = val
        sub_data[field] = (op, parsed_val)
    return sub_data


def parse_date(date_str):
    """Parsează data din format D.MM.YYYY"""
    try:
        return datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        return None