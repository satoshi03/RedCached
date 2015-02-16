# RemCache
RemCache is simple memcached client can be handled like redis client.
In other words, this memcached client has some methods of redis such as "hash" and "string".


## How to install

<pre>
$ sudo pip install -U git+https://github.com/satoshi03/RemCache
</pre>

or from source:

<pre>
$ sudo python setup.py install
</pre>

## Getting started

<pre>
import remcache

> c = remcache.Client(['127.0.0.1:11211'])

> c.set('spam', 10)
1
> c.get('spam')
10
> c.incr('spam')
11
> c.incrby('spam', 70)
81
</pre>

If you want to store data as hash-field like json format data, use hash methods.

<pre>
import remcache

> c = remcache.Client(['127.0.0.1:11211'])
> c.hset('food', 'spam', 80)
1
> c.hset('food', 'ham', 60)
1
> c.hget('food', 'spam')
80
> c.hgetall('food')
{u'ham': 60, u'spam': 80}
</pre>


