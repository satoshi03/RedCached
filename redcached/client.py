# -*- conding: utf-8 -*-


# python std library
import json

# 3rd party library
import memcache


class Client(memcache.Client):

    SUCCESS = 1
    FAILED = 0

    def __init__(self, servers):
        super(Client, self).__init__(servers)

    def hset(self, key, field, value):
        json_val = self.get(key)
        if json_val is not None:
            val = json.loads(json_val)
        else:
            val = {}
        val[field] = value
        return self.set(key, json.dumps(val)) if Client.SUCCESS else Client.FAILED

    def hget(self, key, field):
        json_val = self.get(key)
        if json_val is not None:
            val = json.loads(json_val)
            if field in val:
                return val[field]
        return None

    def hgetall(self, key):
        json_val = self.get(key)
        if json_val is not None:
           return json.loads(json_val)
        return None

    def hvals(self, key):
        json_val = self.get(key)
        if json_val is not None:
            return [val for key, val in json.loads(json_val).iteritems()]
        return None

    def hkeys(self, key):
        json_val = self.get(key)
        if json_val is not None:
            return [key for key, val in json.loads(json_val).iteritems()]
        return None

    def hlen(self, key):
        json_val = self.get(key)
        if json_val is not None:
            return len(json.loads(json_val))
        return 0

    def hexists(self, key, field):
        json_val = self.get(key)
        if json_val is not None:
            return field in json.loads(json_val)
        return False

    def hdel(self, key, *fields):
        json_val = self.get(key)
        ret = Client.FAILED
        if json_val is not None:
            val = json.loads(json_val)
            for field in fields:
                if field in val:
                    ret = Client.SUCCESS
                    val.pop(field)
            if ret is Client.SUCCESS:
                self.set(key, json.dumps(val))
        return ret

    def hincrby(self, key, field, increment):
        if not isinstance(increment, int):
            raise RedCachedException("value is not an integer or out of range")
        json_val = self.get(key)
        if json_val is not None:
            val = json.loads(json_val)
            if field in val:
                if not isinstance(val[field], int):
                    raise RedCachedException("value is not an integer or out of range") 
                val[field] = int(val[field]) + increment
            else:
                val[field] = increment
        else:
            val = { field: increment }
        return self.set(key, json.dumps(val)) if Client.SUCCESS else Client.FAILED


class RedCachedException(Exception):
    def __init__(self, message):
        super(RedCachedException, self).__init__(message)


