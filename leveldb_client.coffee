class LevelDBClient
  constructor: (dir) ->
    @levelup = require('levelup') dir, {
      keyEncoding: 'hex'
      valueEncoding: 'hex'
    }


module.exports = {LevelDBClient}
