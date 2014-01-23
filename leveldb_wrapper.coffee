class LevelDBWrapper
  constructor: (client, opt={}) ->
    @levelup = client.levelup
    @_prefix = opt.prefix or ""

  _applyPrefix: (key) ->
    @_prefix + key

  get: (k, c=(->)) -> @levelup.get @_applyPrefix(k), c
  put: (k, v, c=(->)) -> @levelup.put @_applyPrefix(k), v, c

  put_batch: (rows, c=(->)) ->
    ops = []
    for row in rows
      if row.key?
        {key, value} = row
      else
        [key, value] = row
      key = @_applyPrefix(key)
      ops.push {type: 'put', key, value}
    @levelup.batch ops, c

  get_range: ({prefix, limit}, c=(->)) ->
    limit ?= -1
    return c new Error "prefix required" if not prefix?
    return c new Error "prefix can't be empty" if prefix.length == 0

    start = prefix
    end_bytes = new Buffer start, 'utf8'
    last_byte = end_bytes[end_bytes.length - 1]
    # Note: this simple increment doesn't handle keys ending with FF
    # UTF-8 never contains FF, but keep this in mind when supporting Buffers
    end_bytes[end_bytes.length - 1] = last_byte + 1
    end = end_bytes.toString('utf8')

    start = @_applyPrefix start
    end = @_applyPrefix end

    rows = []
    @levelup.createReadStream({start, end, limit})
      .on('data', (row) =>
        if row.key != end
          row.key = row.key.substr @_prefix.length
          rows.push row)
      .on('error', (e) =>
        c e
        c = null # in case 'close' gets called after this
      )
      .on('close', (() =>
        if c
          c null, rows
      ))


module.exports = {LevelDBWrapper}
