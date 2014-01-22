class LevelDBClient
  constructor: (dir) ->
    @levelup = require('levelup')(dir)


module.exports = {LevelDBClient}
