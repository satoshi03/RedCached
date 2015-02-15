import pytest

from conftest import c


def test_hget_hset(c):
    c.hmset('a', {'1': 1, '2': 2, '3': 3})
    assert c.hget('a', '1') == 1
    assert c.hget('a', '2') == 2
    assert c.hget('a', '3') == 3

    assert c.hset('a', '3', 4) == 1
    assert c.hget('a', '3') == 4
    assert c.hset('a', '4', 4) == 1

def test_hgetall(c):
    c.hmset('a', {'1': 1, '2': 2, '3': 3})
    assert c.hgetall('a') == {'1': 1, '2': 2, '3': 3}

def test_hdel(c):
    c.hmset('a', {'1': 1, '2': 2, '3': 3})
    assert c.hdel('a', '1') == 1
    assert c.hgetall('a') == {'2': 2, '3': 3}
    assert c.hdel('a', '4') == 0
    assert c.hgetall('a') == {'2': 2, '3': 3}
    assert c.hdel('a', '2') == 1
    assert c.hdel('a', '3') == 1
    # if all fields were deleted, key is also deleted
    assert c.hgetall('a') == None
    # if key dose not exist, it returns 0
    assert c.hdel('b', '3') == 0

def test_hexists(c):
    c.hmset('a', {'1': 1, '2': 2, '3': 3})
    assert c.hexists('a', '1') == 1
    assert c.hexists('a', '4') == 0
    # if key dose not exist, it returns 0
    assert c.hexists('b', '4') == 0

def test_hincrby(c):
    c.hmset('a', {'1': 1, '2': 2, '3': 3})
    # gets value after increment
    assert c.hincrby('a', '1', 1) == 2
    assert c.hget('a', '1') == 2
    assert c.hincrby('a', '1', 2) == 4
    assert c.hget('a', '1') == 4

    # if key does not exists, create new key
    assert c.hincrby('b', '1', 1) == 1
    assert c.hincrby('b', '2', 3) == 3

def test_hincrbyfloat(c):
    c.hmset('a', {'1': 1, '2': 2, '3': 3})
    assert c.hincrbyfloat('a', '1', 1.2) == 2.2
    assert c.hget('a', '1') == 2.2

    # if key does not exists, create new key
    assert c.hincrbyfloat('b', '1', 1.2) == 1.2
    assert c.hincrbyfloat('b', '2', 3.3) == 3.3

def test_hkeys(c):
    c.hmset('a', {'1': 1, '2': 2, '3': 3})
    keys = c.hkeys('a')
    keys.sort()
    assert keys == ['1', '2', '3']
    # if key does not exists, return None
    assert c.hkeys('b') == None

def test_hvals(c):
    c.hmset('a', {'1': 1, '2': 2, '3': 3})
    vals = c.hvals('a')
    vals.sort()
    assert vals == [1, 2, 3]
    # if key does not exists, return None
    assert c.hvals('b') == None

def test_hlen(c):
    c.hmset('a', {'1': 1, '2': 2, '3': 3})
    assert c.hlen('a') == 3

def test_hmget(c):
    c.hmset('a', {'1': 1, '2': 2, '3': 3})
    assert c.hmget('a', '1', '2') == [1, 2]
    # if some fields do not exist, a value is skipped
    assert c.hmget('a', '1', '2', '4') == [1, 2]

def test_hsetnx(c):
    c.hmset('a', {'1': 1, '2': 2, '3': 3})
    # if field does not exist, set value
    assert c.hsetnx('a', '4', 4) == 1
    assert c.hget('a', '4') == 4
    # if field exists, does not set value
    assert c.hsetnx('a', '1', 2) == 0
    assert c.hget('a', '1') == 1

def pytest_func_arg__c(request):
        return c()


if __name__ == "__main__":
    pytest.main()
