import RedisStreamsClient from '../src/'

const streamsClient = new RedisStreamsClient({
  post: 6379,
  host: process.env.REDIS_HOST
})

for (let i = 0; i < 10; i++) {
  await streamsClient.publish('mystream', {says: i})
}

const val = await streamsClient.getRange('mystream')
console.log(val)