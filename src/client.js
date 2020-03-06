/* eslint-disable guard-for-in */
/* eslint-disable no-continue */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-restricted-syntax */

// Node dependencies
const os = require('os');

// External dependencies
const IOredis = require('ioredis');
const { compressSync } = require('snappy');

// Internal dependencies
const { mapObjToStringArray, deserialize } = require('./utils');
const {
  FIELD_FOR_COMPRESSED_VALUES, META_PREFIX, PAYLOAD_PREFIX, REDIS_ARRAY_POSITIONS,
} = require('./common-vals');

const READ_GROUP_EVENT_ID = {
  FIRST_RUN: '0',
  NEW_ITEMS: '>',
};

let redis;

function setOptions(options) {
  const defaults = {
    pendingInterval: 60 * 10 * 1000, // 10 minutes
    maxIdleTime: 60 * 120 * 1000, // 2 hours
    consumerName: os.hostname(),
    shouldProcess: true,
    shouldBlock: false,
    maxStreamLength: 5000,
    isConsumer: false,
  };

  return Object.assign(defaults, options);
}

function connect(ctxOptions) {
  if (ctxOptions && ctxOptions.cluster) {
    return new IOredis.Cluster([
      {
        host: ctxOptions.host,
        port: ctxOptions.port,
      },
    ],
    {
      dnsLookup: (address, callback) => callback(null, address),
      redisOptions: {
        tls: {},
        password: ctxOptions.password,
      },
    });
  }
  // non-cluster mode
  return new IOredis(ctxOptions);
}

function validateEventMeta(eventMeta) {
  if (!eventMeta) throw Error('event meta object must be defined');
  if (!eventMeta.type) throw Error('event type must be defined');
  if (!eventMeta.origin) throw Error('event origin must be defined');
}

function formatEventObject(eventObject) {
  const prefix = eventObject && eventObject.type ? META_PREFIX : PAYLOAD_PREFIX;
  const eventObjectFormatted = {};

  for (const [key, value] of Object.entries(eventObject)) {
    eventObjectFormatted[`${prefix}-${key}`] = value;
  }

  return eventObjectFormatted;
}

function combineEventData(eventMeta, eventPayload) {
  return { ...formatEventObject(eventMeta), ...formatEventObject(eventPayload) };
}

function isStreamCreatedButWithoutItems(streamData) {
  return streamData[
    REDIS_ARRAY_POSITIONS.streamRoot
  ][
    REDIS_ARRAY_POSITIONS.root.streamEvents
  ].length === 0;
}

function isStreamEmpty(streamData) {
  if (!streamData || streamData.length === 0 || isStreamCreatedButWithoutItems(streamData)) {
    return true;
  }
  return false;
}

/**
 * Acknowledge the item in the consumer group.
 * This will remove the item from the PEL (pending entries list) of the consumer group.
 *
 * <pre>
 * XACK [stream name] [group name] [id]
 * </pre>
 * @private
 * @param {string} streamName - Name (sometimes referred to as the "key") of the stream
 * @param {string} groupName - Name of the consumer group
 * @param {string} id - ID of the item
 */
function ack({ streamName, groupName, id }) {
  return redis.xack(streamName, groupName, id);
}

/**
 * Add item to the stream.
 * We're using '*' which autogenerates an ID
 * If the stream does not exist, as a side effect of running this command the stream gets created.
 * MAXLEN ~ [max stream length] sets max stream length (in number of items) to cap the stream at.
 * The trimming is performed only when Redis is able to remove a whole macro node.
 * It trims the oldest items.
 *
 * <pre>
 * XADD [stream name] MAXLEN ~ [max stream length] * [item]
 * </pre>
 * @private
 * @param {string} streamName - Name (sometimes referred to as the "key") of the stream
 * @param {string} formattedData - Item data
 */
function add({ streamName, maxStreamLength, formattedData }) {
  return redis.xadd(streamName, 'MAXLEN', '~', maxStreamLength, '*', formattedData);
}

/**
 * Read data from the consumer group.
 *
 * <pre>
 * XREADGROUP GROUP [group name] [consumer name] *COUNT [n]* STREAMS [stream name] >/0
 * </pre>
 * @private
 * @param {string} groupName - Name of the consumer group
 * @param {string} streamName - Name (sometimes referred to as the "key") of the stream
 */
function readFromConsumerGroup({
  groupName, consumerName, streamName, shouldBlock, blockTime = 2000, count = 10, id,
}) {
  return shouldBlock
    ? redis.xreadgroup('GROUP', groupName, consumerName, 'BLOCK', blockTime, 'STREAMS', streamName, id)
    : redis.xreadgroup('GROUP', groupName, consumerName, 'COUNT', count, 'STREAMS', streamName, id);
}

/**
 * This command gets the pending events from the Pending Entries List (PEL) tracked by
 * a consumer group.
 *
 * <pre>
 * XPENDING [stream name] [group name] - + [count] [consumer name]
 * </pre>
 * @private
 * @param {string} streamName - Name (sometimes referred to as the "key") of the stream
 * @param {string} groupName - Name of the consumer group
 * @param {integer} count - Count of pending items to look for
 * @param {string} consumerName - Name of the consumer itself (not the group)
 */
// function getPending({
//   streamName, groupName, count, consumerName,
// }) {
//   return redis.xpending(streamName, groupName, '-', '+', count, consumerName);
// }

/**
 * This command changes the ownership of a pending message,
 * so that the new owner is the consumer specified as the command argument.
 *
 * <pre>
 * XCLAIM [stream name] [group name] [consumer name] [min-idle-time] [id]
 * </pre>
 * @private
 * @param {string} streamName - Name (sometimes referred to as the "key") of the stream
 * @param {string} groupName - Name of the consumer group
 * @param {string} consumerName - Name of the consumer itself (not the group)
 * @param {integer} pendingTime - Idle time of event
 * @param {string} id - ID of the event
 */
// function claim({
//   streamName, groupName, consumerName, pendingTime, id,
// }) {
//   return redis.xclaim(streamName, groupName, consumerName, pendingTime, id);
// }

/**
 * Creates a consumer group.
 * We're using the special ID '$' (that means: the ID of the last item in the stream).
 * In this case the consumers fetching data from that consumer group will only see new elements
 * arriving in the stream, from the point in time in which the group was created.
 * TODO: We may need to provide a way to override that in the future.
 *
 * <pre>
 * XGROUP CREATE [stream name] [group name] $ *[MKSTREAM]*
 * MKSTREAM creates the stream if it doesn't exist yet
 * </pre>
 * @private
 * @param {string} streamName - Name (sometimes referred to as the "key") of the stream
 * @param {string} groupName - Name of the consumer group
 */
function createConsumerGroup(streamName, groupName) {
  // need to make sure that creating a stream post-MKSTREAM doesn't conflict
  return redis.xgroup('CREATE', streamName, groupName, '$', 'MKSTREAM');
}

// TODO: this is still a WIP, and is more pseudo-code at this point
// async function handlePending({
//   streamName, groupName, maxIdleTime, count,
// }) {
//   const pendingItems = getPending({
//     streamName, groupName, count,
//   });

//   for (const pendingItem in pendingItems) {
//     const { itemId, itemIdleTime } = pendingItem;

//     if (itemIdleTime >= maxIdleTime) {
//       try {
//         await claim(streamName, groupName, itemId);
//       } catch (e) {
//         console.warn(`'${this.consumerName}' failed to claim item for ${groupName}: ${e}`);
//       }
//     }
//   }
// }

class EventStreamClient {
  constructor(options) {
    const configuredOptions = setOptions(options);
    Object.assign(this, configuredOptions);

    redis = connect(this.ctxOptions);
  }

  /**
   * Publish an event/item.
   *
   * @param {string} streamName - Name (sometimes referred to as the "key") of the stream
   * @param {object} eventMeta - The event metadata
   * @param {object} eventPayload - The actual event _data_ to be published
   */
  publish({ streamName, eventMeta, eventPayload }) {
    if (typeof streamName !== 'string') throw Error('stream name must be included and must be a string');
    if (typeof eventPayload !== 'object') throw Error('event payload must be included and must be an object');
    validateEventMeta(eventMeta);

    let event;

    if (eventMeta && eventMeta.shouldCompress) {
      try {
        const stringifiedEvent = JSON.stringify(eventPayload);
        const compressedValue = compressSync(stringifiedEvent);
        const base64Version = compressedValue.toString('base64');
        event = combineEventData(eventMeta, { [FIELD_FOR_COMPRESSED_VALUES]: base64Version });
      } catch (err) {
        console.error(`Error compressing data: ${err}`);
      }
    } else {
      event = combineEventData(eventMeta, eventPayload);
    }

    return add({
      streamName,
      maxStreamLength: this.maxStreamLength,
      formattedData: mapObjToStringArray(event),
    });
  }

  async publishMany({ streamName, events, shouldCompress = false }) {
    if (typeof streamName !== 'string') throw Error('streamName must be included and must be a string');
    if (!Array.isArray(events)) throw Error('events must be included and must be an array');

    // TODO: do this in a .pipeline() instead to save network requests
    for (const event of events) {
      const { eventMeta, eventPayload } = event;
      await this.publish({
        streamName, eventMeta, eventPayload, shouldCompress,
      });
    }
  }

  /**
   * Subscribe to a consumer group and process event/item.
   *
   * @param {string} groupName - Name of the consumer group
   * @param {string} streamName - Name (sometimes referred to as the "key") of the stream
   * @param {integer} readTimeout - how long the timeout for the reading of messages and recursion
   * @param {function} workerFunction - The function to process the event/item
   */
  async subscribe({
    groupName, streamName, readTimeout, workerFunction,
  }) {
    if (typeof groupName !== 'string') throw Error('groupName must be included and must be a string');
    if (typeof streamName !== 'string') throw Error('streamName must be included and must be a string');
    if (typeof workerFunction !== 'function') throw Error('workerFunction must be included and must be a function');

    // safety flag to turn off processing of messages if something goes pear-shaped
    if (this.shouldProcess) {
      // ensure consumer group exists and ignore error if it already does
      // (Redis API returns an err for some reason)
      try {
        await createConsumerGroup(streamName, groupName);
      } catch (e) {
        // ignore
      }

      const run = async ({ lastId, checkBacklog }) => {
        // just needed because of how we recursively call this
        let readId = lastId;
        let backlog = checkBacklog;

        let streamData;

        if (!backlog) {
          readId = READ_GROUP_EVENT_ID.NEW_ITEMS;
        }

        try {
          streamData = await readFromConsumerGroup({
            groupName,
            consumerName: this.consumerName,
            streamName,
            shouldBlock: this.shouldBlock,
            blockTime: readTimeout,
            id: readId,
          });
        } catch (e) {
          console.error(`readFromConsumerGroup error: ${e}`);
        }

        if (isStreamEmpty(streamData)) {
          backlog = false;
        } else {
          const deserializedEvents = deserialize(streamData);

          for (const event of deserializedEvents) {
            try {
              const { id, ...data } = event;

              await workerFunction({ ...data });
              await ack({ streamName, groupName, id });
              readId = id;
            } catch (e) {
              console.error(e);
            }
          }
        }

        setTimeout(run.bind(null,
          { lastId: readId, checkBacklog: backlog }),
        readTimeout);
      };

      run({ lastId: READ_GROUP_EVENT_ID.FIRST_RUN, checkBacklog: true });
    }
  }
}

module.exports = EventStreamClient;
