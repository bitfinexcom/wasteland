'use strict'

const _ = require('lodash')
const async = require('async')
const crypto = require('crypto')

const {
  prepareData,
  getMaxPointersPerBuffer,
  getSliceLimit,
  prepareMultiLevelData
} = require('../lib/data-helper.js')

class GrenacheBackend {
  constructor (conf = {}) {
    this.conf = {
      transport: null,

      maxIndirections: 2,
      bufferSizelimit: 1000,
      addressSize: 40, // sha1 length,
      concurrentRequests: 5,
      keys: null
    }

    _.extend(this.conf, conf)

    if (!this.conf.transport) {
      throw new Error('no transport set')
    }

    this.transport = this.conf.transport

    this.maxPointersPerBuffer = getMaxPointersPerBuffer(
      this.conf.bufferSizelimit,
      this.conf.addressSize,
      this.wrapPointers
    )

    this.maxSliceCount = getSliceLimit(
      this.conf.bufferSizelimit,
      this.maxPointersPerBuffer,
      this.conf.maxIndirections
    )
  }

  start (cb = () => {}) {
    this.transport.start()
    cb(null)
  }

  stop (cb = () => {}) {
    this.transport.stop()
    cb(null)
  }

  getPayload (data, opts = {}) {
    const res = {
      v: data
    }

    if (!opts.seq) {
      return res
    }

    // mutable
    if (opts.salt) {
      res.salt = opts.salt
    }

    res.seq = opts.seq

    return res
  }

  put (data, opts, cb) {
    const conf = _.pick(this.conf, [ 'bufferSizelimit' ])

    prepareData(data, conf, (err, slices) => {
      if (err) return cb(err)

      if (slices.length === 1) {
        if (!opts.salt) {
          opts.salt = this.getSha(slices[0])
        }

        this.send(slices[0], opts, cb)
        return
      }

      this.storeChunked(slices, opts, (err, data) => {
        if (err) return cb(err)

        return cb(null, data)
      })
    })
  }

  getSha (data) {
    let sha = crypto.createHash('sha1')
    return sha.update(data + Math.random()).digest('hex')
  }

  storeParallel (slices, opts, cb) {
    const tasks = this.getStoreTasks(slices, opts)
    this.taskParallel(tasks, (err, res) => {
      if (err) return cb(err)

      cb(null, res)
    })
  }

  wrapAndStorePointers (p, opts, cb) {
    const wrapped = this.wrapPointers(p)
    const sha = this.getSha(JSON.stringify(wrapped))
    opts.salt = sha

    this.send(wrapped, opts, cb)
  }

  storeChunked (slices, opts, cb) {
    const maxPointersPerBuffer = this.maxPointersPerBuffer

    if (slices.length < maxPointersPerBuffer) {
      this.storeParallel(slices, opts, (err, pointers) => {
        if (err) return cb(err)

        this.wrapAndStorePointers(pointers, opts, cb)
      })

      return
    }

    if (slices.length > this.maxSliceCount) {
      return cb(new Error('data too large: adjust maxIndirections'))
    }

    const chunks = prepareMultiLevelData(slices, maxPointersPerBuffer)

    this.storeUntilPointersFit(chunks, opts, cb)
  }

  storeUntilPointersFit (slices, opts, cb) {
    const tasks = slices.map((box) => {
      return (cb) => {
        this.storeParallel(box, opts, cb)
      }
    })

    async.series(tasks, (err, res) => {
      if (err) return err

      const flat = _.flatten(res)
      const wrapped = this.wrapPointers(flat)
      this.put(wrapped, opts, cb)
    })
  }

  wrapPointers (pointers) {
    const res = JSON.stringify({
      wasteland_type: 'pointers',
      p: pointers
    })

    return res
  }

  getStoreTasks (slices, opts) {
    const tasks = slices.map((chunk) => {
      return (cb) => {
        opts.salt = this.getSha(chunk)
        this.send(chunk, opts, cb)
      }
    })

    return tasks
  }

  taskParallel (tasks, cb) {
    const { concurrentRequests } = this.conf

    async.parallelLimit(tasks, concurrentRequests, (err, res) => {
      if (err) return cb(err)

      cb(null, res)
    })
  }

  send (data, opts, cb) {
    const payload = this.getPayload(data, opts)

    if (opts.seq !== 0 && !opts.seq) {
      return this.sendImmutable(payload, opts, cb)
    }

    const { keys } = this.conf
    if (!keys) {
      return cb(new Error('no keys set'))
    }

    opts.keys = keys
    return this.sendMutable(payload, opts, cb)
  }

  sendImmutable (payload, opts, cb) {
    if (opts.salt) payload.salt = opts.salt

    this.transport.put(payload, (err, hash) => {
      if (err) return cb(err)

      cb(null, hash)
    })
  }

  sendMutable (payload, opts, cb) {
    this.transport.putMutable(payload, opts, (err, hash) => {
      if (err) return cb(err)

      cb(null, hash)
    })
  }

  isResultChunked (payload) {
    if (!payload) return false

    if (payload.wasteland_type === 'pointers') {
      return true
    }

    return false
  }

  maybeRetrieveChunked (initialPayload, cb) {
    let content

    try {
      content = JSON.parse(initialPayload.v)
    } catch (e) {
      return cb(null, initialPayload)
    }

    if (this.isResultChunked(content)) {
      return this.handleChunked(initialPayload, content.p, cb)
    }

    return cb(null, initialPayload)
  }

  getRetrieveTasks (pointers, opts) {
    const tasks = pointers.map((pointer) => {
      return (cb) => {
        this.get(pointer, { recursive: true }, cb)
      }
    })

    return tasks
  }

  handleChunked (payload, pointers, cb) {
    payload.original = payload.v

    const opts = {}
    const tasks = this.getRetrieveTasks(pointers, opts)

    this.taskParallel(tasks, (err, res) => {
      if (err) return cb(err)

      const reduced = res.reduce((acc, el) => {
        return acc + el.v
      }, '')

      payload.v = reduced

      this.maybeRetrieveChunked(payload, cb)
    })
  }

  get (key, opts = {}, cb) {
    if (!key) return cb(new Error('no key provided'))

    this.transport.get(key, (err, data) => {
      if (err) return cb(err)

      if (opts.recursive) return cb(err, data)

      this.maybeRetrieveChunked(data, (err, res) => {
        if (err) return cb(err)

        return cb(null, res)
      })
    })
  }
}

module.exports = GrenacheBackend
