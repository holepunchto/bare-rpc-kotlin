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
    case 6: {
      const call = rpc.request(7)
      const response = call.createResponseStream()
      call.send(request.data)
      const chunks = []
      for await (const chunk of response) chunks.push(chunk)
      return request.reply(Buffer.concat(chunks))
    }
    case 7: {
      const response = request.createResponseStream()
      response.write(request.data)
      response.end(request.data)
      return
    }
    case 8: {
      const response = request.createResponseStream()
      for await (const chunk of request.createRequestStream()) response.write(chunk)
      response.end()
      return
    }
    case 9: {
      const call = rpc.request(7)
      const response = call.createResponseStream()
      call.send(request.data)
      for await (const chunk of response) return request.reply(chunk)
      return
    }
    case 10: {
      const response = request.createResponseStream()
      response.on('close', () => rpc.event(10).send(request.data))
      flood(response, request.data)
      return
    }
    case 11: {
      const call = rpc.request(7)
      const body = call.createRequestStream()
      body.on('close', () => rpc.event(11).send(request.data))
      flood(body, request.data)
      return request.reply(await call.reply())
    }
    case 12: {
      const call = rpc.request(7)
      const body = call.createRequestStream()
      body.on('close', () => rpc.event(12).send(request.data))
      body.write(request.data)
      return request.reply(await call.reply())
    }
    case 13: {
      const call = rpc.request(7)
      const response = call.createResponseStream()
      call.createRequestStream().end(request.data)
      const chunks = []
      for await (const chunk of response) chunks.push(chunk)
      return request.reply(Buffer.concat(chunks))
    }
    default:
      throw new Error(`Unknown command ${request.command}`)
  }
})

function flood(stream, data) {
  const write = () => {
    if (!stream.destroyed) stream.write(data, () => setImmediate(write))
  }
  write()
}
