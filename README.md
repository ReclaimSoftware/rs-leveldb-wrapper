**Yet another layer to append to the top of the abstraction stack between your code and LevelDB**

```coffee
wrapper = new LevelDBWrapper levelup_client

wrapper.get key, (e, value) ->

wrapper.put key, value, (e) ->

wrapper.get_range {prefix}, (e, rows) ->
  for {key, value} in rows
    ...
```

### [License: MIT](LICENSE.txt)
