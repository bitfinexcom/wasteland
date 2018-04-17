/* eslint-env mocha */

'use strict'

const assert = require('assert')
const { bootTwoGrapes, killGrapes } = require('./helper')

const GrenacheBackend = require('../backends/Grenache.js')
const Link = require('grenache-nodejs-link')
const ed = require('ed25519-supercop')

let grapes
describe('Grenache Storage Backend', () => {
  before(function (done) {
    this.timeout(20000)

    bootTwoGrapes((err, g) => {
      if (err) throw err

      grapes = g
      done()
    })
  })

  after(function (done) {
    this.timeout(5000)

    killGrapes(grapes, done)
  })

  it('gb stores data unchunked', (done) => {
    const link = new Link({
      grape: 'http://127.0.0.1:30001'
    })
    link.start()

    const { publicKey, secretKey } = ed.createKeyPair(ed.createSeed())

    const gb = new GrenacheBackend({
      transport: link,
      keys: { publicKey, secretKey }
    })

    const opts = { seq: 1, salt: 'pineapple-salt' }

    gb.put('unchunked-data', opts, (err, hash) => {
      if (err) throw err

      assert.ok(hash)
      gb.get(hash, {}, (err, data) => {
        if (err) throw err

        assert.equal(data.v, 'unchunked-data')
        assert.ok(data.id)
        assert.ok(data.seq)
        assert.equal(data.salt, 'pineapple-salt')
        assert.equal(data.k, publicKey.toString('hex'))

        link.stop()
        done()
      })
    })
  }).timeout(7000)

  it('gb stores data chunked', (done) => {
    const link = new Link({
      grape: 'http://127.0.0.1:30001'
    })
    link.start()

    const { publicKey, secretKey } = ed.createKeyPair(ed.createSeed())

    const gb = new GrenacheBackend({
      transport: link,
      keys: { publicKey, secretKey },
      bufferSizelimit: 1000
    })

    const longString = new Array(1005).join('a')

    const opts = { seq: 1 }

    gb.put(longString, opts, (err, hash) => {
      if (err) throw err

      assert.ok(hash)
      gb.get(hash, {}, (err, data) => {
        if (err) throw err

        assert.equal(data.v, longString)
        assert.ok(data.id)
        assert.ok(data.seq)
        assert.ok(data.salt)
        assert.equal(data.k, publicKey.toString('hex'))

        link.stop()
        done()
      })
    })
  }).timeout(20000)

  it('gb stores data chunked with indirections', (done) => {
    const link = new Link({
      grape: 'http://127.0.0.1:30001'
    })
    link.start()

    const { publicKey, secretKey } = ed.createKeyPair(ed.createSeed())

    const gb = new GrenacheBackend({
      transport: link,
      keys: { publicKey, secretKey },
      bufferSizelimit: 1000
    })

    const longString = new Array(22000).join('a')

    const opts = { seq: 1 }

    gb.put(longString, opts, (err, hash) => {
      if (err) throw err

      assert.ok(hash)
      gb.get(hash, {}, (err, data) => {
        if (err) throw err

        assert.equal(data.v, longString)
        assert.ok(data.id)
        assert.ok(data.seq)
        assert.ok(data.salt)
        assert.equal(data.k, publicKey.toString('hex'))

        link.stop()
        done()
      })
    })
  }).timeout(20000)

  it('gb stores data chunked with indirections, large data', (done) => {
    const link = new Link({
      grape: 'http://127.0.0.1:30001'
    })
    link.start()

    const { publicKey, secretKey } = ed.createKeyPair(ed.createSeed())

    const gb = new GrenacheBackend({
      transport: link,
      keys: { publicKey, secretKey },
      bufferSizelimit: 1000
    })

    const longString = new Array(2200000).join('a')

    const opts = { seq: 1 }

    gb.put(longString, opts, (err, hash) => {
      if (err) throw err

      assert.ok(hash)
      gb.get(hash, {}, (err, data) => {
        if (err) throw err

        assert.equal(data.v, longString)
        assert.ok(data.id)
        assert.ok(data.seq)
        assert.ok(data.salt)
        assert.equal(data.k, publicKey.toString('hex'))

        link.stop()
        done()
      })
    })
  }).timeout(20000)

  it('verifies sequences', (done) => {
    const link = new Link({
      grape: 'http://127.0.0.1:30001'
    })
    link.start()

    const { publicKey, secretKey } = ed.createKeyPair(ed.createSeed())

    const gb = new GrenacheBackend({
      transport: link,
      keys: { publicKey, secretKey },
      bufferSizelimit: 1000
    })

    const opts = { seq: 1, salt: 'pineapple-salt' }

    gb.put('furbie', opts, (err, hash) => {
      if (err) throw err
      gb.put('furbie', opts, (err, hash) => {
        assert.ok(err) // outdated sequence
        assert.equal(err.message, '302 sequence number less than current')

        opts.seq = 2 // increase seq
        gb.put('furbie-foo', opts, (err, hash) => {
          if (err) throw err
          gb.get(hash, {}, (err, data) => {
            if (err) throw err
            assert.equal(data.seq, 2)
            assert.equal(data.v, 'furbie-foo')

            link.stop()
            done()
          })
        })
      })
    })
  }).timeout(20000)

  it('stores immutable data', (done) => {
    const link = new Link({
      grape: 'http://127.0.0.1:30001'
    })
    link.start()

    const gb = new GrenacheBackend({
      transport: link,
      bufferSizelimit: 1000
    })

    const opts = {} // no sequence provided, data is stored immutable
    gb.put('furbie', opts, (err, hash) => {
      if (err) throw err

      gb.put('furbie', opts, (err, hash2) => {
        if (err) throw err
        assert.equal(hash, hash2) // hash/key stays same

        gb.put('furbie-foo', opts, (err, hash3) => {
          if (err) throw err
          // different content, different hash/key
          assert.notEqual(hash2, hash3)
          assert.notEqual(hash, hash3)

          link.stop()
          done()
        })
      })
    })
  }).timeout(20000)
})
