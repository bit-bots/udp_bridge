from bitbots_test.test_case import TestCase
from hypothesis import assume, given
from hypothesis.strategies import text

from udp_bridge import aes_helper


class AesHelperTestCase(TestCase):
    @given(text(), text())
    def test_decrypt_inverts_encrypt(self, message, key):
        assume(message != "")

        enc_text = aes_helper.AESCipher(key).encrypt(message)
        dec_text = aes_helper.AESCipher(key).decrypt(enc_text)

        self.assertEqual(message, dec_text)


if __name__ == "__main__":
    from bitbots_test import run_unit_tests

    run_unit_tests(AesHelperTestCase)
