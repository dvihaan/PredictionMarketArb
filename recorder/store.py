"""Where data lives: a local directory (development) or an S3 bucket and prefix (production).

Both expose the same small interface, with keys relative to the root, using "/" separators:
    list(prefix) -> sorted keys    read(key) -> bytes    write(key, bytes)    exists(key) -> bool
    list_meta(prefix) -> sorted [(key, bytes, modified_ms)]
"""
import os


def open_store(location, region=None):
    """location is a local directory or s3://bucket/optional/prefix."""
    if location.startswith("s3://"):
        bucket, _, prefix = location[5:].partition("/")
        return S3Store(bucket, prefix, region)
    return LocalStore(location)


class LocalStore:
    def __init__(self, root):
        self.root = root

    def _path(self, key):
        return os.path.join(self.root, *key.split("/"))

    def list(self, prefix):
        base = self._path(prefix)
        out = []
        for root, _, files in os.walk(base):
            for name in files:
                full = os.path.join(root, name)
                out.append(os.path.relpath(full, self.root).replace(os.sep, "/"))
        return sorted(out)

    def list_meta(self, prefix):
        out = []
        for key in self.list(prefix):
            st = os.stat(self._path(key))
            out.append((key, st.st_size, int(st.st_mtime * 1000)))
        return out

    def read(self, key):
        with open(self._path(key), "rb") as f:
            return f.read()

    def write(self, key, data):
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)

    def exists(self, key):
        return os.path.exists(self._path(key))

    def __repr__(self):
        return f"LocalStore({self.root})"


class S3Store:
    def __init__(self, bucket, prefix="", region=None):
        import boto3
        self.bucket, self.prefix = bucket, prefix.strip("/")
        self.s3 = boto3.client("s3", region_name=region)

    def _key(self, key):
        return f"{self.prefix}/{key}" if self.prefix else key

    def list(self, prefix):
        return [key for key, _, _ in self.list_meta(prefix)]

    def list_meta(self, prefix):
        out, strip = [], len(self.prefix) + 1 if self.prefix else 0
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self._key(prefix)):
            out += [(o["Key"][strip:], o["Size"], int(o["LastModified"].timestamp() * 1000))
                    for o in page.get("Contents", [])]
        return sorted(out)

    def read(self, key):
        return self.s3.get_object(Bucket=self.bucket, Key=self._key(key))["Body"].read()

    def write(self, key, data):
        self.s3.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)

    def exists(self, key):
        try:
            self.s3.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except self.s3.exceptions.ClientError:
            return False

    def __repr__(self):
        return f"S3Store(s3://{self.bucket}/{self.prefix})"
