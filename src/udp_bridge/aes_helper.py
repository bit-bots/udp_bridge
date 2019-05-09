import base64
import hashlib
from Crypto import Random
from Crypto.Cipher import AES


class AESCipher:
    """
    A wrapper around the true python AESCipher.

    This wrapper takes care of properly padding messages, selection an encryption mode and handling the encryption key.
    It is safe to keep one object because the internal python cipher is not reused.
    """

    def __init__(self, key):
        """
        :param key: The passphrase used to encrypt and decrypt messages.
            If it is None, no encryption/decryption takes place
        :type key: str
        """
        if key is not None and len(key) == 0:
            key = None

        if key is not None:
            self.bs = AES.block_size
            self.mode = AES.MODE_CBC
            self.key = hashlib.sha256(key.encode()).digest()
            self.random = Random.new()
        else:
            self.key = key

    def encrypt(self, message):
        """
        :type message: str
        :rtype: bytes
        """
        if self.key is not None:
            raw = self._pad(message)
            iv = self.random.read(AES.block_size)
            cipher = AES.new(self.key, self.mode, iv)
            return iv + cipher.encrypt(raw)
        else:
            return message

    def decrypt(self, enc):
        """
        :type enc: bytes
        :rtype: bytes
        """
        if self.key is not None:
            #enc = base64.b64decode(enc)
            iv = enc[:AES.block_size]
            cipher = AES.new(self.key, self.mode, iv)
            dec_msg = cipher.decrypt(enc[AES.block_size:])
            return self._unpad(dec_msg)
        else:
            return enc

    def _pad(self, s):
        return s + (self.bs - len(s) % self.bs) * chr(self.bs - len(s) % self.bs)

    def _unpad(self, s):
        return s[:-ord(s[len(s) - 1:])]
