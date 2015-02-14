#-*- conding: utf-8 -*-

# python std library
import json

# 3rd party library
import memcache


KEY_TYPE_FIELD = "key_type"
KEY_TYPE_HSET = "hset"
KEY_TYPE_SET = "set"


class Client(memcache.Client):

    SUCCESS = 1
    FAILED = 0

    def __init__(self, servers):
        super(Client, self).__init__(servers)

    def hset(self, key, field, value):
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
        else:
            val = {}
        val[field] = value
        val[KEY_TYPE_FIELD] = KEY_TYPE_HSET
        return Client.SUCCESS if self.set(key, json.dumps(val)) else Client.FAILED

    def hsetnx(self, key, field, value):
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
        else:
            val = {}
        if field not in val:
            val[field] = value
            val[KEY_TYPE_FIELD] = KEY_TYPE_HSET
            return Client.SUCCESS if self.set(key, json.dumps(val)) else Client.FAILED
        return Client.FAILED

    def hget(self, key, field):
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
            if field in val:
                return val[field]
        return None

    def hgetall(self, key):
        json_val = self.get(key)
        if json_val is not None:
            return self._get_value_from_json(json_val, KEY_TYPE_HSET)
        return None

    def hvals(self, key):
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
            return [v for k, v in val.iteritems()]
        return None

    def hkeys(self, key):
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
            return [k for k, v in val.iteritems()]
        return None

    def hlen(self, key):
        json_val = self.get(key)
        if json_val is not None:
            return len(self._get_value_from_json(json_val, KEY_TYPE_HSET))
        return 0

    def hexists(self, key, field):
        json_val = self.get(key)
        if json_val is not None:
            return field in self._get_value_from_json(json_val, KEY_TYPE_HSET)
        return False

    def hdel(self, key, *fields):
        json_val = self.get(key)
        ret = Client.FAILED
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
            for field in fields:
                if field in val:
                    ret = Client.SUCCESS
                    val.pop(field)
            if ret is Client.SUCCESS:
                val[KEY_TYPE_FIELD] = KEY_TYPE_HSET
                self.set(key, json.dumps(val))
        return ret

    def hincrby(self, key, field, increment):
        if not isinstance(increment, int):
            raise RedCachedException("value is not an integer or out of range")
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
            if field in val:
                if not isinstance(val[field], int):
                    raise RedCachedException("value is not an integer or out of range")
                val[field] = int(val[field]) + increment
            else:
                val[field] = increment
        else:
            val = { field: increment }
        val[KEY_TYPE_FIELD] = KEY_TYPE_HSET
        return Client.SUCCESS if self.set(key, json.dumps(val)) else Client.FAILED

    def hincrbyfloat(self, key, field, increment):
        if not isinstance(increment, (int, float)):
            raise RedCachedException("value is not an integer or float or out of range")
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
            if field in val:
                if not isinstance(val[field], (int, float)):
                    raise RedCachedException("value is not an integer or out of range")
                val[field] = float(val[field]) + increment
            else:
                val[field] = increment
        else:
            val = { field: increment }
        val[KEY_TYPE_FIELD] = KEY_TYPE_HSET
        return Client.SUCCESS if self.set(key, json.dumps(val)) else Client.FAILED

    def hmget(self, key, *fields):
        json_val = self.get(key)
        ret = []
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
            for field in fields:
                if field in val:
                    ret.append(val[field])
            return ret
        return None

    def hmset(self, key, mapping):
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
        else:
            val = {}
        for k, v in mapping.iteritems():
            val[k] = v
        val[KEY_TYPE_FIELD] = KEY_TYPE_HSET
        return Client.SUCCESS if self.set(key, json.dumps(val)) else Client.FAILED

    def incr(self, key):
        val = self.get(key)
        if val is not None:
           if not isinstance(val, int):
                raise RedCachedException("value is not an integer or out of range")
           val = int(val) + 1
        else:
            val = 1
        return Client.SUCCESS if self.set(key, val) else Client.FAILED

    def _get_value_from_json(self, json_val, key_type):
        try:
            val = json.loads(json_val)
        except TypeError:
            raise RedCachedException("WRONGTYPE Operation against a key holding the wrong kind of value")
        if not self._is_valid_key_type(val, key_type):
            raise RedCachedException("WRONGTYPE Operation against a key holding the wrong kind of value")
        val.pop(KEY_TYPE_FIELD)
        return val

    def _is_valid_key_type(self, val, key_type):
        if KEY_TYPE_FIELD in val:
            return val[KEY_TYPE_FIELD] == key_type
        return False

class RedCachedException(Exception):
    def __init__(self, message):
        super(RedCachedException, self).__init__(message)


