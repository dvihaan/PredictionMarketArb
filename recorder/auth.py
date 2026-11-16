"""Kalshi API key loading and request signing. Never logs or prints the private key.

Kalshi signs timestamp_ms + METHOD + path (without the query string) with the account's private key:
RSA-PSS with SHA-256 and a digest-length salt, or Ed25519. The signature is base64-encoded.
"""
import base64
import os
import time

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa

SSM_KEY_ID = "/prediction-market-arb/kalshi/key-id"
SSM_PRIVATE_KEY = "/prediction-market-arb/kalshi/private-key"


class KalshiKey:
    def __init__(self, key_id, pem_bytes):
        self.key_id = key_id.strip()
        self.key = serialization.load_pem_private_key(pem_bytes.strip(), password=None)
        if not isinstance(self.key, (rsa.RSAPrivateKey, ed25519.Ed25519PrivateKey)):
            raise TypeError(f"unsupported Kalshi key type {type(self.key).__name__}")

    @classmethod
    def from_ssm(cls, region=None, key_id_param=SSM_KEY_ID, private_key_param=SSM_PRIVATE_KEY):
        import boto3
        ssm = boto3.client("ssm", region_name=region)
        get = lambda name: ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]  # noqa: E731
        return cls(get(key_id_param), get(private_key_param).encode())

    @classmethod
    def from_file(cls, key_id, path):
        with open(os.path.expanduser(path), "rb") as f:
            return cls(key_id, f.read())

    def sign(self, text):
        data = text.encode()
        if isinstance(self.key, ed25519.Ed25519PrivateKey):
            sig = self.key.sign(data)
        else:
            sig = self.key.sign(data, padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                                                  salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
        return base64.b64encode(sig).decode()

    def headers(self, method, path):
        ts = str(int(time.time() * 1000))
        return {"KALSHI-ACCESS-KEY": self.key_id, "KALSHI-ACCESS-TIMESTAMP": ts,
                "KALSHI-ACCESS-SIGNATURE": self.sign(ts + method + path.split("?")[0])}

    def __repr__(self):
        return f"KalshiKey({self.key_id[:8]}…, {type(self.key).__name__})"
