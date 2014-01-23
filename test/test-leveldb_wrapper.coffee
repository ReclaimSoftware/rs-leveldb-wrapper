assert = require 'assert'
async = require 'async'
{LevelDBClient, LevelDBWrapper} = require '../index'

describe "LevelDBWrapper", () ->

  client = new LevelDBClient "#{__dirname}/temp"
  wrapper = new LevelDBWrapper client, prefix: "p:"


  describe "get", () ->

    it "gets", (done) ->
      wrapper.put 'foo', 'bar', (e) ->
        wrapper.get 'foo', (e, value) ->
          assert.equal e, null
          assert.equal value, 'bar'
          done()


  describe "put", () ->

    it "puts", (done) ->
      wrapper.put 'x', 'y', (e) ->
        assert.equal e, null
        wrapper.get 'x', (e, value) ->
          assert.equal value, 'y'
          done()


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
          assert.deepEqual rows, [
            {key: 'x:', value: 'v2'}
            {key: 'x:a', value: 'v3'}
            {key: 'x:b', value: 'v4'}
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
          assert.deepEqual rows, [
            {key: 'x:', value: 'v2'}
            {key: 'x:a', value: 'v3'}
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
