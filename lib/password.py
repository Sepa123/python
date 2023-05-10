# from passlib.context import CryptContext
import hashlib


def hash_password(password):
    hash = hashlib.md5(password.encode("utf-8")).hexdigest().upper()
    return hash

def verify_password(form_password,check_password):
    form_password = hash_password(form_password)

    if (form_password == check_password):
        print(True)
        return True   
    print(False)
    return False
# crypt = CryptContext(schemes=["bcrypt"])

# def check_password(password):
#     return crypt.hash(password)

# def verify_password(plain_password, hashed_password):
#     return crypt.verify(plain_password, hashed_password)