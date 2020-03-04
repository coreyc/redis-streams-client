import os from 'os'

import RedisStreamsClient from '../src/'

const streamsClient = new RedisStreamsClient({
  port: 6379,
  host: process.env.REDIS_HOST
})

const STREAM_NAME = 'mystream'
const GROUP_NAME = os.hostname()

const processItem = async ([id, data]) => {
  const random = Math.floor(Math.random() * 10)
  // fail 20% of the time
  if (random === 0 || random === 1) throw Error('random simulated failure')
  else console.log('item processed:', id, 'data:', data)
}

const run = async () => {
  await streamsClient.subscribe(GROUP_NAME, STREAM_NAME, processItem)
}


run()
