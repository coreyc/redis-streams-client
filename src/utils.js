/* eslint-disable no-restricted-syntax */
/* eslint-disable guard-for-in */
const { uncompressSync } = require('snappy');

const {
  FIELD_FOR_COMPRESSED_VALUES,
  PAYLOAD_PREFIX,
  REDIS_ARRAY_POSITIONS,
} = require('./common-vals');

const mapObjToStringArray = (obj) => {
  if (!obj || typeof obj !== 'object') throw new Error('must pass in object');

  const arr = [];

  for (const key in obj) {
    arr.push(key, obj[key]);
  }

  return arr;
};

const mapArrayToObject = (array) => {
  if (!Array.isArray(array)) throw Error('expects an array as argument');

  const arrayCopy = array.slice();
  const object = {};

  while (arrayCopy && arrayCopy.length) {
    const key = arrayCopy.shift();
    const value = arrayCopy.shift();
    object[key] = value;
  }

  return object;
};

const copyArray = (array) => array.slice();

const getRootStreamData = (streamData) => streamData[REDIS_ARRAY_POSITIONS.streamRoot];

const getStreamEvents = (rootStreamData) => rootStreamData[REDIS_ARRAY_POSITIONS.root.streamEvents];

const getEventId = (streamEvent) => streamEvent[REDIS_ARRAY_POSITIONS.streamEvent.eventId];

const getEventData = (streamEvent) => streamEvent[REDIS_ARRAY_POSITIONS.streamEvent.eventData];

const uncompressEventData = (eventData) => eventData.map((item, index) => {
  // key in k/v pair indicated value is compressed
  const isCompressed = eventData[index - 1] === `${PAYLOAD_PREFIX}-${FIELD_FOR_COMPRESSED_VALUES}`;
  if (isCompressed) {
    const bufferedValue = Buffer.from(item, 'base64');
    return uncompressSync(bufferedValue, { asBuffer: false });
  }
  return item;
});

const deserialize = (streamData) => {
  const streamDataCopy = copyArray(streamData);
  const rootStreamData = getRootStreamData(streamDataCopy);
  const streamEvents = getStreamEvents(rootStreamData);

  return streamEvents.map((streamEvent) => {
    const eventId = getEventId(streamEvent);

    const eventData = getEventData(streamEvent);
    const uncompressedEventData = uncompressEventData(eventData);
    const deserializedFromRedis = mapArrayToObject(uncompressedEventData);

    return {
      id: eventId,
      ...deserializedFromRedis,
    };
  });
};

module.exports = {
  mapObjToStringArray,
  deserialize,
};
