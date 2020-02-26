import RedisStreamsClient from '../src/'

const streamsClient = new RedisStreamsClient(6379, 'redis')

const STREAM_NAME = 'mystream'
const GROUP_NAME = 'mygroup'

const processItem = async (item) => {
  const random = Math.floor(Math.random() * 10)
  // fail 20% of the time
  if (random === 0 || random === 1) throw Error('random simulated failure')
  else console.log('item processed:', JSON.stringify(item))
}

const run = async () => {
  await streamsClient.subscribe(GROUP_NAME, STREAM_NAME, processItem)
}

// cleanup step just for testing
await streamsClient.deleteConsumerGroup(STREAM_NAME, GROUP_NAME)
// purposefully don't await
run()

// setInterval(() => {
//   console.log('interval to simulate other processing in thread')
// }, 500)