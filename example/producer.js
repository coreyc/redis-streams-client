import RedisStreamsClient from '../src/'

const streamsClient = new RedisStreamsClient(6379, "redis")

await streamsClient.publish('mystream', {says: 'hiiiii there'})
const val = await streamsClient.getRange('mystream')
console.log(val)