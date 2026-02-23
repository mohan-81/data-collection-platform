# this is the platform security layer for encrypting/decrypting sensitive fields in the database

from security.crypto import encrypt_value, decrypt_value

# fields that must ALWAYS be encrypted
SECRET_FIELDS = [
    "client_secret",
    "api_key",
    "access_token",
    "refresh_token",
    "password",
    "bot_token",
    "config_json"
]

# ENCRYPT BEFORE INSERT/UPDATE
def encrypt_payload(data: dict):
    secured = {}
    for k, v in data.items():
        if k in SECRET_FIELDS and v:
            secured[k] = encrypt_value(v)
        else:
            secured[k] = v
    return secured

# DECRYPT AFTER FETCH (similar to auto_decrypt_row; use one consistently)
def decrypt_payload(row: dict):
    if not row:
        return row
    for k in SECRET_FIELDS:
        if k in row and row[k]:
            row[k] = decrypt_value(row[k])
    return row