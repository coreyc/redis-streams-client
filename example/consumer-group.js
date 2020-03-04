import os from 'os'

import RedisStreamsClient from '../src/'

const streamsClient = new RedisStreamsClient({
  port: 6379,
  host: process.env.REDIS_HOST
})

const STREAM_NAME = 'mystream'
const GROUP_NAME = os.hostname()

const processItem = async ([id, data]) => {
  console.log('item processed:', id, 'data:', data)
}

const run = async () => {
  await streamsClient.subscribe(GROUP_NAME, STREAM_NAME, processItem)
}

// cleanup step just for testing
// await streamsClient.deleteConsumerGroup(STREAM_NAME, GROUP_NAME)

run()
