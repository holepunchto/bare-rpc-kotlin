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
      if (request.reply) throw new Error('Command 3 is an event')
      return rpc.event(3).send(request.data)
    case 4: {
      const chunks = []
      for await (const chunk of request.createRequestStream()) chunks.push(chunk)
      return request.reply(Buffer.concat(chunks))
    }
    case 5: {
      const call = rpc.request(7)
      call.createRequestStream().end(request.data)
      return request.reply(await call.reply())
    }
    default:
      throw new Error(`Unknown command ${request.command}`)
  }
})
