import pytest

import remcache


def _get_client(cls):
    params = ['127.0.0.1:11211']
    c = cls(params)
    c.flush_all()
    return c

@pytest.fixture()
def c():
    return _get_client(remcache.Client)
