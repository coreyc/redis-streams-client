import RedisStreamsClient from '../src/'

const streamsClient = new RedisStreamsClient(6379, 'redis')

let val

const run = async () => {
  while (true) {
    val = await streamsClient.read('mystream', 0)
    console.log(JSON.stringify(val))
    if (!val) continue
  }
}

// purposefully don't await
run()

setInterval(() => {
  console.log('interval')
}, 500)