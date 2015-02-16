import pytest

from conftest import c


def test_get(c):
    c.set('a', '1')
    assert c.get('a') == '1'

def test_incr(c):
    c.set('a', 1)
    assert c.incr('a') == 2
    # if key does not exist, create new key with 1
    assert c.incr('b') == 1

def test_incrby(c):
    c.set('a', 1)
    assert c.incrby('a', 1) == 2
    assert c.incrby('a', 2) == 4
    # if key does not exist, create new key with assigned value
    assert c.incrby('b', 2) == 2

def test_incrbyfloat(c):
    c.set('a', 1)
    assert c.incrbyfloat('a', 1.1) == 2.1
    assert c.incrbyfloat('a', 2.1) == 4.2
    # if key does not exist, create new key with assigned value
    assert c.incrbyfloat('b', 2.1) == 2.1

def test_getset(c):
    c.set('a', 1)
    assert c.getset('a', 2) == 1
    assert c.getset('a', 3) == 2
    assert c.getset('b', 4) == None

def test_mset(c):
    c.mset({'a': 1, 'b': 2, 'c': 'string'})
    assert c.get('a') == 1
    assert c.get('b') == 2
    assert c.get('c') == 'string'

def test_msetnx(c):
    c.mset({'a': 1, 'b': 2, 'c': 'string'})
    # if at least one key already exsits, it does not update and return 0
    assert c.msetnx({'a': 1, 'd': 3, 'e': 4}) == 0
    assert c.msetnx({'d': 3, 'e': 4}) == 1
    assert c.get('d') == 3
    assert c.get('e') == 4

def test_setnx(c):
    c.mset({'a': 1, 'b': 2, 'c': 'string'})
    assert c.setnx('d', 3) == 1
    assert c.get('d') == 3
    assert c.setnx('a', 2) == 0

"""
# NOT IMPLEMENTED
def test_mget(c):
    c.set('a', 1)
    c.set('b', 2)
    c.set('c', 3)
    assert c.mget('a', 'b') == [1, 2]
    assert c.mget('b', 'c') == [2, 3]
"""

def pytest_func_arg__c(request):
        return c()


if __name__ == "__main__":
    pytest.main()
