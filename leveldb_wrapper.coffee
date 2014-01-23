class LevelDBWrapper
  constructor: (client, opt={}) ->
    @levelup = client.levelup
    @_prefix = opt.prefix or ""
    @_prefix = new Buffer @_prefix, 'utf8' if not Buffer.isBuffer @_prefix

  _applyPrefix: (key) ->
    key = new Buffer key, 'utf8' if not Buffer.isBuffer key
    Buffer.concat [@_prefix, key]

  get: (key, c=(->)) ->
    key = @_applyPrefix(key)
    @levelup.get key.toString('hex'), c

  put: (key, value, c=(->)) ->
    key = @_applyPrefix(key)
    value = new Buffer value, 'utf8' if not Buffer.isBuffer value
    @levelup.put key.toString('hex'), value.toString('hex'), c

  put_batch: (rows, c=(->)) ->
    ops = []
    for row in rows
      if row.key?
        {key, value} = row
      else
        [key, value] = row
      key = @_applyPrefix(key)
      key = new Buffer key, 'utf8' if not Buffer.isBuffer key
      value = new Buffer value, 'utf8' if not Buffer.isBuffer value
      ops.push {type: 'put', key, value}
    @levelup.batch ops, c

  get_range: ({prefix, limit}, c=(->)) ->
    limit ?= -1
    return c new Error "prefix required" if not prefix?
    return c new Error "prefix can't be empty" if prefix.length == 0
    prefix = new Buffer prefix, 'utf8' if not Buffer.isBuffer prefix
    return c new Error "prefix can't (yet) end with FF" if prefix[prefix.length - 1] == 0xFF
    # (due to our super-naive incrementing to make the inclusive range end)

    start = prefix
    end = new Buffer start.length
    start.copy end
    end[end.length - 1] = end[end.length - 1] + 1
    start = @_applyPrefix start
    end = @_applyPrefix end

    rows = []
    @levelup.createReadStream({start, end, limit})
      .on('data', (row) =>
        {key, value} = row
        if key.toString('hex') != end.toString('hex')
          key = key.slice @_prefix.length
          rows.push [key, value])
      .on('error', (e) =>
        c e
        c = null # in case 'close' gets called after this
      )
      .on('close', (() =>
        if c
          c null, rows
      ))


module.exports = {LevelDBWrapper}
