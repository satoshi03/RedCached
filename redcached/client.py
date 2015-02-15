#-*- conding: utf-8 -*-

# python std library
import json

# 3rd party library
import memcache


VALUE_FIELD = "values"
KEY_TYPE_FIELD = "key_type"
KEY_TYPE_STRING = "string"
KEY_TYPE_HSET = "hash"
KEY_TYPE_SET = "set"
KEY_TYPE_LIST = "list"
KEY_TYPE_SORTED_SET = "sorted_set"


class Client(memcache.Client):

    SUCCESS = 1
    FAILED = 0

    def __init__(self, servers):
        super(Client, self).__init__(servers)

    def hset(self, key, field, value):
        """
        Set the string value of a hash field
        """
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
        else:
            val = {}
        val[field] = value
        return self._set_value_to_json(key, val, KEY_TYPE_HSET)

    def hsetnx(self, key, field, value):
        """
        Set the value of a hash field, only if the field does not exist
        """
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
        else:
            val = {}
        if field not in val:
            val[field] = value
            return self._set_value_to_json(key, val, KEY_TYPE_HSET)
        return Client.FAILED

    def hget(self, key, field):
        """
        Get the value of a hash field
        """
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
            if field in val:
                return val[field]
        return None

    def hgetall(self, key):
        """
        Get all the fields and values in a hash
        """
        json_val = self.get(key)
        if json_val is not None:
            return self._get_value_from_json(json_val, KEY_TYPE_HSET)
        return None

    def hvals(self, key):
        """
        Get all the values in a hash
        """
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
            return [v for k, v in val.iteritems()]
        return None

    def hkeys(self, key):
        """
        Get all the fields in a hash
        """
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
            return [k for k, v in val.iteritems()]
        return None

    def hlen(self, key):
        """
        Get the number of fields in a hash
        """
        json_val = self.get(key)
        if json_val is not None:
            return len(self._get_value_from_json(json_val, KEY_TYPE_HSET))
        return 0

    def hexists(self, key, field):
        """
        Determine if a hash field exists
        """
        json_val = self.get(key)
        if json_val is not None:
            return field in self._get_value_from_json(json_val, KEY_TYPE_HSET)
        return False

    def hdel(self, key, *fields):
        """
        Delete one or more hash fields
        """
        json_val = self.get(key)
        ret = Client.FAILED
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
            for field in fields:
                if field in val:
                    ret = Client.SUCCESS
                    val.pop(field)
            if ret is Client.SUCCESS:
                self.set(key, json.dumps(val))
        return ret

    def hincrby(self, key, field, increment):
        """
        Increment the integer value of a hash field by the given number
        """
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
        return val[field] if self._set_value_to_json(key, val, KEY_TYPE_HSET) else Client.FAILED

    def hincrbyfloat(self, key, field, increment):
        """
        Increment the float value of a hash field by the given amount
        """
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
        return val[field] if self._set_value_to_json(key, val, KEY_TYPE_HSET) else Client.FAILED

    def hmget(self, key, *fields):
        """
        Get the values of all the given hash fields
        """
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
        """
        Set multiple hash fields to multiple values
        """
        json_val = self.get(key)
        if json_val is not None:
            val = self._get_value_from_json(json_val, KEY_TYPE_HSET)
        else:
            val = {}
        for k, v in mapping.iteritems():
            val[k] = v
        return self._set_value_to_json(key, val, KEY_TYPE_HSET)

    def incr(self, key):
        """
        Increment the integer value of a key by one
        """
        val = self.get(key)
        if val is not None:
            if not isinstance(val, int):
                raise RedCachedException("value is not an integer or out of range")
            val = int(val) + 1
        else:
            val = 1
        return val if self.set(key, val) else Client.FAILED

    def incrby(self, key, increment):
        """
        Increment the integer value of a key by the given amount
        """
        if not isinstance(increment, int):
            raise RedCachedException("value is not an integer or out of range")
        val = self.get(key)
        if val is not None:
            if not isinstance(val, int):
                raise RedCachedException("value is not an integer or out of range")
            val = int(val) + increment
        else:
            val = increment
        return val if self.set(key, val) else Client.FAILED

    def incrbyfloat(self, key, increment):
        """
        Increment the float value of a key by the given amount
        """
        if not isinstance(increment, (int, float)):
            raise RedCachedException("value is not an integer or float or out of range")
        val = self.get(key)
        if val is not None:
            if not isinstance(val, (int, float)):
                raise RedCachedException("value is not an integer or out of range")
            val = float(val) + increment
        else:
            val = increment
        return val if self.set(key, val) else Client.FAILED

    def decr(self, key):
        """
        Decrement the integer value of a key by one
        """
        val = self.get(key)
        if val is not None:
           if not isinstance(val, int):
                raise RedCachedException("value is not an integer or out of range")
           val = int(val) - 1
        else:
            val = -1
        return val if self.set(key, val) else Client.FAILED

    def mset(self, mapping):
        """
        Set multiple keys to multiple values
        """
        success_flg = 0 # if value is 0, it indicates success
        for key, val in mapping.iteritems():
            success_flg += 0 if self.set(key, val) else 1
        return Client.SUCCESS if success_flg == 0 else Client.FAILED

    def getset(self, key, val):
        """
        Set the string value of a key and return its old value
        """
        retval = self.get(key)
        if KEY_TYPE_STRING == self._get_key_type(retval) or retval is None:
            self.set(key, val)
            return retval
        else:
            raise RedCachedException("WRONGTYPE Operation against a key holding the wrong kind of value")

    def strlen(self, key):
        """
        Get the length of the value stored in a key
        """
        val = self.get(key)
        if val is None:
            return 0
        if KEY_TYPE_STRING == self._get_key_type(val):
            return len(str(val))
        else:
            raise RedCachedException("WRONGTYPE Operation against a key holding the wrong kind of value")

    def set(self, key, val):
        """
        Set the string value of a key
        Override from set on memcache client
        """
        return Client.SUCCESS if super(Client, self).set(key, val) else Client.FAILED

    def setnx(self, key, val):
        """
        Set the value and expiration of a key
        """
        val = self.get(key)
        if val is None:
            return self.set(key, val)
        else:
            return Client.FAILED

    def msetnx(self, mapping):
        """
        Set multiple keys to multiple values, only if none of the keys exist
        """
        # if at least one key already existed, no key is set
        for key, val in mapping.iteritems():
            if self.get(key) is not None:
                return Client.FAILED
        for key, val in mapping.iteritems():
            self.set(key, val)
        return Client.SUCCESS

    def type(self, key):
        """
        Determine the type stored at key
        """
        val = self.get(key)
        if val is not None:
            return self._get_key_type(val)
        return None

    def _get_value_from_json(self, json_val, key_type):
        try:
            val = json.loads(json_val)
        except (TypeError, ValueError):
            raise RedCachedException("WRONGTYPE Operation against a key holding the wrong kind of value")
        if not self._is_valid_key_type(val, key_type):
            raise RedCachedException("WRONGTYPE Operation against a key holding the wrong kind of value")
        return val[VALUE_FIELD]

    def _is_valid_key_type(self, val, key_type):
        if KEY_TYPE_FIELD in val:
            return val[KEY_TYPE_FIELD] == key_type
        return False

    def _get_key_type(self, json_val):
        try:
            val = json.loads(json_val)
            if KEY_TYPE_FIELD in val:
                return val[KEY_TYPE_FIELD]
        except (TypeError, ValueError):
            pass
        return KEY_TYPE_STRING

    def _set_value_to_json(self, key, val, key_type):
        dic = {}
        dic[VALUE_FIELD] = val
        dic[KEY_TYPE_FIELD] = key_type
        return self.set(key, json.dumps(dic))

class RedCachedException(Exception):
    def __init__(self, message):
        super(RedCachedException, self).__init__(message)


