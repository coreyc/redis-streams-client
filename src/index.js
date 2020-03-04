import os from 'os'

import IOredis from 'ioredis'

import { mapObjToStringArray } from '../util'

process.env.SHOULD_PROCESS = true
process.env.GROUP_NAME

// xreadgroup puts items onto PEL
// xack removes those items from the PEL

// TODO: pull out private functions to top and don't export, bind in class
// TODO: order methods alphabetically or by similar action

export default class RedisStreamsClient {
  constructor(options) {
    this.connect(options)
    this.pending_time = 60000
    this.consumer_name = options.consumer_name || this.getConsumerName()
  }

  connect(options) {
    // this.getRedisModule(options)
    try {
      this.redis = new IOredis(options)
    } catch (e) {
      console.error(`Error connecting to redis: ${e}`)
    }
  }

  getRedisModule() {
    // allow for node-redis and ioredis?
  }

  ack(stream_name, group_name, id) {
    // XACK [stream name] [group name] [id]
    return this.redis.xack(stream_name, group_name, id)
  }

  // should this be part of the API?
  removeStaleConsumer() {
    // for when a consumer hasn't checked in in a while

    // use hash and set auto-delete / expire?

    // who should call this? dont necessarily want to run it from within another fn ie before every read
    // bc that's an extra fn call
    // cron job?

    // XGROUP DELCONSUMER mystream consumer-group-name myconsumer123
  }

  getConsumerName() {
    // should this be a uuid for safety? rely on xclaim to pick up and assume container will get new consumer_name everytime?
    
    // what should we fall back to if os.hostname() fails? uuid?
    // CAN os.hostname() fail?
    return os.hostname()
  }

  // TODO: better optional arg ordering
  getRange(stream_name, start = '-', end = '+') {
    // XRANGE key/stream_name start end *[COUNT count]*
    return this.redis.xrange(stream_name, start, end)
  }

  publish(stream_name, data) {
    const formatted_data = mapObjToStringArray(data)

    /*
    * XADD
    * key = stream_name
    * ID = *
    * field value [field value ...] = formatted_data
    */

    return this.redis.xadd(stream_name, '*', formatted_data)
  }

  read(stream_name, id, should_block) {
    return should_block
      ? this.redis.xread('BLOCK', 0, 'STREAMS', stream_name, id)
      : this.redis.xread('STREAMS', stream_name, id)
  }

  readFromConsumerGroup(group_name, stream_name) {
    // XREADGROUP GROUP [group name] *COUNT [n]* [consumer name] STREAMS [stream name] >/0
    return this.redis.xreadgroup('GROUP', group_name, this.consumer_name, 'COUNT', 1, 'STREAMS', stream_name, '>')
  }

  // **** ONLY for testing purposes!!!! will remove from API later ******
  deleteConsumerGroup(stream_name, consumer_group) {
    // XGROUP DESTROY mystream consumer-group-name
    return this.redis.xgroup('DESTROY', stream_name, consumer_group)
  }

  getPending(stream_name, group_name, count) {
    // XPENDING [stream name] [group name] - + [count] [consumer name]
    return this.redis.xpending(stream_name, group_name, '-', '+', count, this.consumer_name)
  }

  claim(stream_name, group_name, id) {
    // XCLAIM [stream name] [group name] [consumer name] [min pending time] [id]
    return this.redis.xclaim(stream_name, group_name, this.consumer_name, this.pending_time, id)
  }

  createConsumerGroup(stream_name, group_name) {
    // XGROUP CREATE [stream name] [group name] $ *[MKSTREAM]*
    // MKSTREAM creates the stream if it doesn't exist yet
    // need to make sure that creating a stream post-MKSTREAM doesn't conflict
    return this.redis.xgroup('CREATE', stream_name, group_name, '$')
  }

  deformat(stream_data) {
    // TODO: quick hack, make it better
    return stream_data.pop()[1]
  }

  // TODO: should this method be up to the client to implement?
  async subscribe(group_name, stream_name, processItem) {
    // safety flag to turn off processing of messages if something goes pear-shaped
    if (process.env.SHOULD_PROCESS) {

      // ensure consumer group exists and ignore error if it already does
      try {
        await this.createConsumerGroup(stream_name, group_name)
      } catch (e) {
        // ignore
      }

      while (true) {
        const stream_data = await this.readFromConsumerGroup(group_name, stream_name)

        if (!stream_data || stream_data.length === 0) {
          continue
        } else {
          const formatted_data = this.deformat(stream_data)

          for (const item of formatted_data) {
            try {
              const [id, data] = item

              await processItem(item)
              await this.ack(stream_name, group_name, id)

              continue
            } catch (e) {
              console.error(e)
              // TODO: throw it back? or handle here
              continue
            }
          }
        }
      }
    }
  }
}
