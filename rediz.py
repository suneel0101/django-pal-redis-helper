import redis
from django.conf import settings

redis_connection = redis.from_url(settings.REDIS_CONNECTION)


class BaseRedisHelper(object):

    def __init__(self, *args, **kwargs):
        # We need to maintain one single pipe per instance, but we don't
        # want to hit redis without actually using it
        self._pipe = None

    @property
    def pipe(self):
        if self._pipe is None:
            self._pipe = self.conn.pipeline()
        return self._pipe

    @property
    def conn(self):
        return redis_connection

    def _prefix(self, key):
        """
        Namespace all keys with prefix.
        Also namespace all keys with 'testing' if in a test.
        """
        if self.prefix:
            key = u'{}:{}'.format(self.prefix, key)
        return key

    # List methods
    def lpush(self, value, key=None):
        key = key or self.key
        return self.conn.lpush(self._prefix(key), value)

    def lrem(self, value, key=None):
        key = key or self.key
        return self.conn.lrem(self._prefix(key), 0, value)

    # String methods
    def set(self, value, key=None):
        key = key or self.key
        return self.conn.set(self._prefix(key), value)

    def setnx(self, value, key=None):
        key = key or self.key
        return self.conn.setnx(self._prefix(key), value)

    def get(self, key=None, pipe=False):
        key = key or self.key
        if pipe:
            return self.pipe.get(self._prefix(key))
        else:
            return self.conn.get(self._prefix(key))

    def incr(self, count=1, key=None):
        key = key or self.key
        return self.conn.incr(self._prefix(key), count)

    # Set methods
    def smembers(self, key=None):
        key = key or self.key
        return list(self.conn.smembers(self._prefix(key)))

    def sismember(self, value, key=None):
        key = key or self.key
        return self.conn.sismember(self._prefix(key), value)

    def sadd(self, value, key=None):
        key = key or self.key
        return self.conn.sadd(self._prefix(key), value)

    def srem(self, value, key=None):
        key = key or self.key
        return self.conn.srem(self._prefix(key), value)

    def zadd(self, score, value, key=None):
        key = key or self.key
        return self.conn.zadd(self._prefix(key), score, value)

    @property
    def zcard(self, key=None):
        key = key or self.key
        return self.conn.zcard(self._prefix(key))

    # Hash methods
    def hset(self, field, value, key=None):
        key = key or self.key
        return self.conn.hset(self._prefix(key), field, value)

    def hget(self, field, key=None):
        key = key or self.key
        return self.conn.hget(self._prefix(key), field)

    def hgetall(self, key=None):
        key = key or self.key
        return self.conn.hgetall(self._prefix(key))

    # Key methods
    def delete(self, key):
        key = key or self.key
        return self.conn.delete(self._prefix(key))

    def expire(self, expiration, key=None):
        key = key or self.key
        return self.conn.expire(self._prefix(key), expiration)
