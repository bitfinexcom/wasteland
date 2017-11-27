'use strict'

const Wasteland = require('../')
const MemoryBackend = require('../backends/Memory.js')

const ed = require('ed25519-supercop')
const { publicKey, secretKey } = ed.createKeyPair(ed.createSeed())

const mb = new MemoryBackend({
  keys: { publicKey, secretKey }
})

const wl = new Wasteland({ backend: mb })

const opts = { seq: 1, salt: 'salt123' }

wl.put('furbie', opts, (err, hash) => {
  if (err) throw err

  wl.get(hash, opts, (err, data) => {
    if (err) throw err

    console.log('response:')
    console.log(data)
  })
})
