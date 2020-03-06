const FIELD_FOR_COMPRESSED_VALUES = 'compressed';
const META_PREFIX = 'meta';
const PAYLOAD_PREFIX = 'payload';
const REDIS_ARRAY_POSITIONS = {
  streamRoot: 0,
  root: {
    streamName: 0,
    streamEvents: 1,
  },
  streamEvent: {
    eventId: 0,
    eventData: 1,
  },
};

module.exports = {
  FIELD_FOR_COMPRESSED_VALUES,
  META_PREFIX,
  PAYLOAD_PREFIX,
  REDIS_ARRAY_POSITIONS,
};
