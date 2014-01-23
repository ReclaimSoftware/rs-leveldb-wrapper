assert = require 'assert'
async = require 'async'
{LevelDBClient, LevelDBWrapper} = require '../index'

describe "LevelDBWrapper", () ->

  client = new LevelDBClient "#{__dirname}/temp"
  wrapper = new LevelDBWrapper client, prefix: "p:"


  describe "get", () ->

    it "gets a Buffer", (done) ->
      wrapper.put 'foo', 'bar', (e) ->
        wrapper.get 'foo', (e, value) ->
          assert.ok Buffer.isBuffer value
          assert.equal e, null
          assert.equal value.toString('utf8'), 'bar'
          done()


  describe "put", () ->

    it "puts", (done) ->
      wrapper.put 'x', 'y', (e) ->
        assert.equal e, null
        wrapper.get 'x', (e, value) ->
          assert.equal value.toString('utf8'), 'y'
          done()


  describe "put/get", () ->

    it "puts/gets binary values", (done) ->
      wrapper.put 'x', (new Buffer [0x00, 0xC0, 0xFF, 0xEE]), (e) ->
        assert.equal e, null
        wrapper.get 'x', (e, value) ->
          assert.ok Buffer.isBuffer value
          assert.equal value.toString('hex'), '00c0ffee'
          done()

    it "puts/gets binary keys", (done) ->
      wrapper.put (new Buffer [0x00, 0xC0, 0xFF, 0xEE]), 'v00COFFEE', (e) ->
        assert.equal e, null
        wrapper.get (new Buffer [0x00, 0xC0, 0xFF, 0xEE]), (e, value) ->
          assert.equal value.toString('utf8'), 'v00COFFEE'
          done()


  describe "put_batch", () ->
    xit()


  describe "get_range", () ->

    it "gets a range", (done) ->
      async.series [
        ((c) -> wrapper.put 'x9', 'v1', c)
        ((c) -> wrapper.put 'x:', 'v2', c)
        ((c) -> wrapper.put 'x:b', 'v4', c)
        ((c) -> wrapper.put 'x:a', 'v3', c)
        ((c) -> wrapper.put 'x;', 'v5', c)
      ], () ->
        wrapper.get_range {prefix: 'x:'}, (e, rows) ->
          assert.ok not e
          assert.deepEqual utf8ify_rows(rows), [
            ['x:', 'v2']
            ['x:a', 'v3']
            ['x:b', 'v4']
          ]
          done()

    it "can be limited", (done) ->
      async.series [
        ((c) -> wrapper.put 'x9', 'v1', c)
        ((c) -> wrapper.put 'x:', 'v2', c)
        ((c) -> wrapper.put 'x:b', 'v4', c)
        ((c) -> wrapper.put 'x:a', 'v3', c)
        ((c) -> wrapper.put 'x;', 'v5', c)
      ], () ->
        wrapper.get_range {prefix: 'x:', limit: 2}, (e, rows) ->
          assert.ok not e
          assert.deepEqual utf8ify_rows(rows), [
            ['x:', 'v2']
            ['x:a', 'v3']
          ]
          done()

    it "errors if you don't specify a prefix", (done) ->
      wrapper.get_range {}, (e) ->
        assert.equal e.message, "prefix required"
        done()

    it "errors if you specify an empty prefix", (done) ->
      wrapper.get_range {prefix: ""}, (e) ->
        assert.equal e.message, "prefix can't be empty"
        done()


utf8ify_rows = (rows) ->
  for [key, value] in rows
    [
      key.toString 'utf8'
      value.toString 'utf8'
    ]
