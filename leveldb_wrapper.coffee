class LevelDBWrapper
  constructor: (@levelup) ->

  get: (k, c=(->)) -> @levelup.get k, c
  put: (k, v, c=(->)) -> @levelup.put k, v, c

  get_range: ({prefix}, c=(->)) ->
    return c new Error "prefix required" if not prefix?
    return c new Error "prefix can't be empty" if prefix.length == 0

    start = prefix
    end_bytes = new Buffer start, 'utf8'
    last_byte = end_bytes[end_bytes.length - 1]
    # Note: this simple increment doesn't handle keys ending with FF
    # UTF-8 never contains FF, but keep this in mind when supporting Buffers
    end_bytes[end_bytes.length - 1] = last_byte + 1
    end = end_bytes.toString('utf8')

    rows = []
    @levelup.createReadStream({start, end})
      .on('data', (row) ->
        if row.key != end
          rows.push row)
      .on('error', (e) ->
        c e
        c = null # in case 'close' gets called after this
      )
      .on('close', (() ->
        if c
          c null, rows
      ))


module.exports = {LevelDBWrapper}
