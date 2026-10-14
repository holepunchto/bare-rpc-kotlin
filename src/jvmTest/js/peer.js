'use strict'

const { Duplex } = require('stream')
const RPC = require('bare-rpc')

const stream = Duplex.from({ readable: process.stdin, writable: process.stdout })

new RPC(stream, (request) => {
  switch (request.command) {
    case 0:
      return request.reply(request.data)
    default:
      throw new Error(`Unknown command ${request.command}`)
  }
})
