const { compressSync } = require('snappy');
const deepEqualInAnyOrder = require('deep-equal-in-any-order');
const chai = require('chai');

chai.use(deepEqualInAnyOrder);

const { expect } = chai;

const {
  mapObjToStringArray,
  deserialize,
  getPendingItemsFormatted,
} = require('../src/utils');
const { FIELD_FOR_COMPRESSED_VALUES, PAYLOAD_PREFIX } = require('../src/common-vals');

describe('utils', () => {
  describe('#mapObjToStringArray', () => {
    it('should map an object\'s key and value to array format', () => {
      const convertedString = mapObjToStringArray({ key: 'value' });

      expect(convertedString).to.deep.equal(['key', 'value']);
    });

    it('should map an object\'s keys and values to array format for multiple keys', () => {
      const convertedString = mapObjToStringArray({
        key1: 'value1',
        key2: 'value2',
      });

      expect(convertedString).to.deep.equalInAnyOrder(['key1', 'value1', 'key2', 'value2']);
    });
  });

  describe('#deserialize', () => {
    it('should deserialize', () => {
      const streamData = [
        ['mystream', [
          ['1582419017278-0', ['field1', 'value1']],
          ['1582419017278-1', ['field1', 'value1', 'field2', 'value2']],
        ]],
      ];

      const deserialized = deserialize(streamData);

      expect(deserialized).to.deep.equalInAnyOrder([
        { id: '1582419017278-0', field1: 'value1' },
        { id: '1582419017278-1', field1: 'value1', field2: 'value2' },
      ]);
    });

    it('should deserialize including compressed items', () => {
      // not sure why Buffer.alloc() isn't working, just using snappy's compress for now
      const stringToCompress = 'buffered value';
      const bufferedValue = compressSync(stringToCompress);

      const streamData = [
        ['mystream', [
          ['1582419017278-0', [`${PAYLOAD_PREFIX}-${FIELD_FOR_COMPRESSED_VALUES}`, bufferedValue]],
          ['1582419017278-1', ['field1', 'value1', 'field2', 'value2']],
        ]],
      ];

      const deserialized = deserialize(streamData);

      expect(deserialized).to.deep.equalInAnyOrder([
        { id: '1582419017278-0', [`${PAYLOAD_PREFIX}-${FIELD_FOR_COMPRESSED_VALUES}`]: stringToCompress },
        { id: '1582419017278-1', field1: 'value1', field2: 'value2' },
      ]);
    });
  });

  describe('#getPendingItemsFormatted', () => {
    it('should format the pending items in an array of objects', () => {
      const pendingItems = [ [ '1675834832880-0', 'consumer1', 51, 1 ] ]
      const pendingItemsFormatted = getPendingItemsFormatted(pendingItems)

      expect(pendingItemsFormatted).to.deep.equalInAnyOrder([
        {
          itemId: '1675834832880-0',
          consumerName: 'consumer1',
          timeSinceLastDelivery: 51,
          numDeliveries: 1,
        }
      ])
    })
  });
});
