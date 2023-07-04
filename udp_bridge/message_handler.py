import base64
import pickle

from typing import Optional
from udp_bridge.aes_helper import AESCipher


class MessageHandler:
    PACKAGE_DELIMITER = b"\xff\xff\xff"

    def __init__(self, encryption_key: Optional[str]):
        self.cipher = AESCipher(encryption_key)

    def encrypt_and_encode(self, data: dict) -> bytes:
        serialized_data = base64.b64encode(pickle.dumps(data, pickle.HIGHEST_PROTOCOL)).decode("ASCII")
        return self.cipher.encrypt(serialized_data)

    def dencrypt_and_decode(self, msg: bytes):
        decrypted_msg = self.cipher.decrypt(msg)
        binary_msg = base64.b64decode(decrypted_msg)
        return pickle.loads(binary_msg)
