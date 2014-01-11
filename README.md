**Yet another layer to append to the top of the abstraction stack between your code and LevelDB**

[![Build Status](https://secure.travis-ci.org/ReclaimSoftware/rs-leveldb-wrapper.png)](http://travis-ci.org/ReclaimSoftware/rs-leveldb-wrapper)

```coffee
wrapper = new LevelDBWrapper levelup_client

wrapper.get key, (e, value) ->

wrapper.put key, value, (e) ->

wrapper.get_range {prefix}, (e, rows) ->
  for {key, value} in rows
    ...
```

### [License: MIT](LICENSE.txt)
