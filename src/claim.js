import os from 'os'

import IOredis from 'ioredis'

process.env.MAX_PENDING_IDLE_TIME = 60 * 60 * 1000 // 1 hour

// TODO: connection pool? share connection?
// TODO: should this be separate from the client API?

export default class RedisStreamsClaimer {
  constructor(options) {
    this.connect(options)
    this.max_idle_time = process.env.MAX_PENDING_IDLE_TIME
    this.consumer_name = options.consumer_name || this.getConsumerName()
  }

  connect(options) {
    try {
      this.redis = new IOredis(options)
    } catch (e) {
      console.error(`Error connecting to redis: ${e}`)
    }
  }

  getConsumerName() {
    return os.hostname()
  }

  getPending(stream_name, group_name, count = 10) {
    // XPENDING [stream name] [group name] - + [count] [consumer name]
    return this.redis.xpending(stream_name, group_name, '-', '+', count, this.consumer_name)
  }

  claim(stream_name, group_name, id) {
    // XCLAIM [stream name] [group name] [consumer name] [min pending time] [id]
    return this.redis.xclaim(stream_name, group_name, this.consumer_name, this.pending_time, id)
  }

  async handlePending(stream_name, group_name, count) {
    const pending_items = this.getPending(stream_name, group_name, count)

    for (const pending_item in pending_items) {
      const { item_id, item_idle_time } = pending_item
      
      if (item_idle_time >= this.max_idle_time) {
        try {
          await this.claim(stream_name, group_name, item_id)
        } catch (e) {
          console.error(`'${this.consumer_name}' failed to claim pending stream item for stream '${stream_name}' and '${group_name}': ${e}`)
        }
      }
    }
  }

  removeStaleConsumer(stream_name, group_name) {
    // for when a consumer hasn't checked in in a while

    // use hash and set auto-delete / expire?

    // who should call this? dont necessarily want to run it from within another fn ie before every read
    // bc that's an extra fn call
    // cron job?

    // XGROUP DELCONSUMER [stream name] [group name] [consumer name]
    return this.redis.xgroup('DELCONSUMER', stream_name, group_name, this.consumer_name)
  }
}
