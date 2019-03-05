import base64
import hashlib
from Crypto import Random
from Crypto.Cipher import AES


class AESCipher:
    def __init__(self, key):
        """
        :param key: The passphrase used to encrypt and decrypt messages.
            If it is None, no encryption/decryption takes place
        :type key: str
        """
        if key is not None and len(key) == 0:
            key = None

        if key is not None:
            self.bs = 32
            self.mode = AES.MODE_CBC
            self.key = hashlib.sha256(key.encode()).digest()
            self.random = Random.new()
        else:
            self.key = key

    def encrypt(self, message):
        """
        :type message: str
        :rtype: str
        """
        if self.key is not None:
            raw = self._pad(message)
            iv = self.random.read(AES.block_size)
            cipher = AES.new(self.key, self.mode, iv)
            return base64.b64encode(iv + cipher.encrypt(raw))
        else:
            return message

    def decrypt(self, enc):
        """
        :type enc: str
        :rtype: str
        """
        if self.key is not None:
            enc = base64.b64decode(enc)
            iv = enc[:AES.block_size]
            cipher = AES.new(self.key, self.mode, iv)
            return self._unpad(cipher.decrypt(enc[AES.block_size:])).decode("UTF-8")
        else:
            return enc

    def _pad(self, s):
        return s + (self.bs - len(s) % self.bs) * chr(self.bs - len(s) % self.bs)

    def _unpad(self, s):
        return s[:-ord(s[len(s) - 1:])]
