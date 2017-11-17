'use strict'

const Wasteland = require('../')
const GrenacheBackend = require('../backends/Grenache.js')

const Link = require('grenache-nodejs-link')
const ed = require('ed25519-supercop')

const link = new Link({
  grape: 'http://127.0.0.1:30001'
})
link.start()

const { publicKey, secretKey } = ed.createKeyPair(ed.createSeed())

const gb = new GrenacheBackend({
  transport: link,
  keys: { publicKey, secretKey }
})

const wl = new Wasteland({ backend: gb })

const opts = { seq: 1, salt: 'pineapple-salt' }

wl.put('unchunked-data', opts, (err, hash) => {
  if (err) throw err

  wl.get(hash, {}, (err, data) => {
    if (err) throw err

    console.log(data)
    link.stop()
  })
})
