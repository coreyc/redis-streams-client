import os from 'os'

import RedisStreamsClaimer from '../src/claim'

const streamsClaimer = new RedisStreamsClaimer({
  port: 6379,
  host: process.env.REDIS_HOST
})

const STREAM_NAME = 'mystream'
const GROUP_NAME = os.hostname()

const run = () => {
  setInterval(async () => {
    await streamsClaimer.handlePending(STREAM_NAME, GROUP_NAME)
  }, 2000)
}

run()
