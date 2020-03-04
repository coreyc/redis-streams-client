import RedisStreamsClient from '../src/'

const streamsClient = new RedisStreamsClient({
  port: 6379,
  host: process.env.REDIS_HOST
})

const STREAM_NAME = 'mystream'

// setInterval(async () => {
  for (let i = 0; i < 10; i++) {
    await streamsClient.publish(STREAM_NAME, {says: i})

    const val = await streamsClient.getRange(STREAM_NAME)
    console.log(val)
  }
// }, 2000)
