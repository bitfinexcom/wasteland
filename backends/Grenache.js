'use strict'

const _ = require('lodash')
const async = require('async')
const crypto = require('crypto')

const {
  prepareData,
  getMaxPointersPerBuffer
} = require('../lib/data-helper.js')

class GrenacheBackend {
  constructor (conf = {}) {
    this.conf = {
      transport: null,

      maxIndirections: 3,
      bufferSizelimit: 1000,
      addressSize: 40, // sha1 length,
      concurrentRequests: 5,
      keys: null
    }

    _.extend(this.conf, conf)

    if (!this.conf.transport) {
      throw new Error('no transport set')
    }

    if (!this.conf.keys) {
      throw new Error('no keys set')
    }

    this.transport = this.conf.transport

    this.maxPointersPerBuffer = getMaxPointersPerBuffer(
      this.conf.bufferSizelimit,
      this.conf.addressSize,
      this.wrapPointers
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

    // mutable
    if (opts.salt) {
      res.salt = opts.salt
    }

    res.seq = opts.seq

    return res
  }

  put (data, opts, cb) {
    if (!_.isFunction(cb)) return cb(new Error('no callback provided'))

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
    return sha.update(data).digest('hex')
  }

  storeChunked (slices, opts, cb) {
    if (slices.length < this.maxPointersPerBuffer) {
      const tasks = this.getStoreTasks(slices, opts)
      this.taskParallel(tasks, (err, res) => {
        if (err) return cb(err)

        const wrapped = this.wrapPointers(res)
        const sha = this.getSha(JSON.stringify(wrapped))
        opts.salt = sha
        this.send(wrapped, opts, cb)
      })

      return
    }

    throw new Error('chunking with more layers not implemented')
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

    const { keys } = this.conf

    opts.keys = keys
    return this.sendMutable(payload, opts, cb)
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

      cb(null, payload)
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
