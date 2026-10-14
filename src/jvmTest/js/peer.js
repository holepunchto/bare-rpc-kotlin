'use strict'

const { Duplex } = require('stream')
const RPC = require('bare-rpc')

const stream = Duplex.from({ readable: process.stdin, writable: process.stdout })

const rpc = new RPC(stream, async (request) => {
  switch (request.command) {
    case 0:
      return request.reply(request.data)
    case 1: {
      const err = new Error('boom')
      err.code = 'E_PEER'
      err.errno = 7
      throw err
    }
    case 2: {
      const call = rpc.request(7)
      call.send(request.data)
      return request.reply(await call.reply())
    }
    case 3:
      return rpc.event(3).send(request.data)
    default:
      throw new Error(`Unknown command ${request.command}`)
  }
})
