/* eslint-disable no-unused-expressions */
/* eslint-disable no-await-in-loop */
/* eslint-disable no-restricted-syntax */
const IORedis = require('ioredis');
const chai = require('chai');
const deepEqualInAnyOrder = require('deep-equal-in-any-order');

chai.use(deepEqualInAnyOrder);

const { expect } = chai;

const EventStreamClient = require('../src');
const { FIELD_FOR_COMPRESSED_VALUES, META_PREFIX, PAYLOAD_PREFIX } = require('../src/common-vals');

const eventMetaType = 'test';
const eventMetaOrigin = 'test-runner';

const defaultEventMeta = {
  type: eventMetaType,
  origin: eventMetaOrigin,
};

const redis = new IORedis(); // so we can drop into to make assertions independently from the client

// helpers
const resetStream = (streamName) => redis.del(streamName);
const getRange = (streamName, count) => redis.xrange(streamName, '-', '+', 'COUNT', count);
const getPending = (streamName, groupName) => redis.xpending(streamName, groupName);
const getFormatted = (events) => {
  const formatted = [];
  for (const event of events) {
    formatted.push(event[1]);
  }
  return formatted;
};
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

describe('EventStreamClient', () => {
  let streamClient;
  const streamName = 'test-stream';

  beforeEach(async () => {
    streamClient = new EventStreamClient();
    await resetStream(streamName);
  });

  describe('#publish', () => {
    it('should publish an event including meta formatting', async () => {
      const eventPayload = {
        test: 'this is a publish test',
      };

      await streamClient.publish({ streamName, eventMeta: defaultEventMeta, eventPayload });
      const eventFromStream = await getRange(streamName, 1);
      const formatted = getFormatted(eventFromStream);

      expect(formatted).to.deep.equal([
        [
          `${META_PREFIX}-type`,
          eventMetaType,
          `${META_PREFIX}-origin`,
          eventMetaOrigin,
          `${PAYLOAD_PREFIX}-test`,
          'this is a publish test',
        ],
      ]);
    });

    it('should publish a compressed event', async () => {
      const eventMeta = {
        type: eventMetaType,
        origin: eventMetaOrigin,
        shouldCompress: true,
      };
      const eventPayload = {
        test: 'this is a publish test',
      };

      await streamClient.publish({ streamName, eventMeta, eventPayload });
      const eventFromStream = await getRange(streamName, 1);
      const formatted = getFormatted(eventFromStream);

      expect(formatted[0]).to.include(`${PAYLOAD_PREFIX}-${FIELD_FOR_COMPRESSED_VALUES}`,
        'IYB7InRlc3QiOiJ0aGlzIGlzIGEgcHVibGlzaCB0ZXN0In0=');
    });

    it('should set the max length of the stream', async () => {
      const numItemsToAdd = 400;
      const maxStreamLength = 100;
      const streamClient2 = new EventStreamClient({ maxStreamLength });

      for (let i = 1; i <= numItemsToAdd; i += 1) {
        const eventPayload = { i };
        await streamClient2.publish({ streamName, eventMeta: defaultEventMeta, eventPayload });
      }

      const length = await redis.xlen(streamName);
      // because the stream gets trimmed by macro node and not exact #, we can't rely on exact
      // length being equal to maxStreamLength
      expect(length < 200).to.be.true;
    });

    it('should set complain if no event meta is passed', async () => {
      const eventPayload = {
        test: 'this is a publish test',
      };

      expect(() => streamClient.publish({ streamName, eventPayload })).to.throw();
    });

    it('should set complain if no event payload is passed', async () => {
      expect(() => streamClient.publish({ streamName, eventMeta: defaultEventMeta })).to.throw();
    });
  });

  describe('#publishMany', () => {
    it('should publish multiple events', async () => {
      const events = [
        {
          eventMeta: defaultEventMeta,
          eventPayload: {
            test1: 'this is a publish test',
          },
        },
        {
          eventMeta: defaultEventMeta,
          eventPayload: {
            test2: 'this is a publish test',
          },
        },
      ];

      await streamClient.publishMany({ streamName, events });

      // specify count as 10 just to pull a few, even though we publish less as part of this test
      const eventsFromStream = await getRange(streamName, 10);

      const formatted = getFormatted(eventsFromStream);

      expect(formatted[0]).to.include(`${PAYLOAD_PREFIX}-test1`, 'this is a publish test');
      expect(formatted[1]).to.include(`${PAYLOAD_PREFIX}-test2`, 'this is a publish test');
    });
  });

  describe('#subscribe', () => {
    it('should subscribe to a consumer group and process the event', async () => {
      // we need unique group names just for testing purposes
      // as the recursive loop that runs as part of subscribe will still be running after each test
      // meaning that another test might end up processing this test's events
      const groupName = Date.now().toString();

      const workerFunction = () => {};
      await streamClient.subscribe({
        groupName, streamName, readTimeout: 20, workerFunction,
      });

      const eventPayload = {
        test: 'this is a subscribe test',
      };
      const streamClient2 = new EventStreamClient();
      await streamClient2.publish({ streamName, eventMeta: defaultEventMeta, eventPayload });
      // to provide buffer for acks and getPending -
      // since acks happen in subscribe and it runs in a setTimeout loop,
      // potential for race condition, but only bc of way we assert!!!
      await sleep(500);

      // we can't spy on private functions (workerFunction is private w/in the closure in subscribe
      // but testing there are items, and they've been acked (not on PEL) works just as well
      const eventFromStream = await getRange(streamName, 1);
      const formatted = getFormatted(eventFromStream);
      expect(formatted[0]).to.include(`${PAYLOAD_PREFIX}-test`, 'this is a subscribe test');

      const pending = await getPending(streamName, groupName);
      expect(pending[0]).to.equal(0);
    });

    it('should subscribe to a consumer group and process a compressed event', async () => {
      // we need unique group names just for testing purposes
      // as the recursive loop that runs as part of subscribe will still be running after each test
      // meaning that another test might end up processing this test's events
      const groupName = Date.now().toString();

      const workerFunction = () => {};
      await streamClient.subscribe({
        groupName, streamName, readTimeout: 20, workerFunction,
      });

      const eventMeta = {
        type: eventMetaType,
        origin: eventMetaOrigin,
        shouldCompress: true,
      };
      const eventPayload = {
        test: 'this is a subscribe test',
      };
      const streamClient2 = new EventStreamClient();
      await streamClient2.publish({ streamName, eventMeta, eventPayload });
      // to provide buffer for acks and getPending -
      // since acks happen in subscribe and it runs in a setTimeout loop,
      // potential for race condition, but only bc of way we assert!!!
      await sleep(500);

      // we can't spy on private functions (workerFunction is private w/in the closure in subscribe
      // but testing there are items, and they've been acked (not on PEL) works just as well
      const eventFromStream = await getRange(streamName, 1);
      const formatted = getFormatted(eventFromStream);
      expect(formatted[0]).to.include(`${PAYLOAD_PREFIX}-${FIELD_FOR_COMPRESSED_VALUES}`,
        'I4h7InRlc3QiOiJ0aGlzIGlzIGEgc3Vic2NyaWJlIHRlc3QifQ==');

      const pending = await getPending(streamName, groupName);
      expect(pending[0]).to.equal(0);
    });

    it('should process multiple events', async () => {
      // we need unique group names just for testing purposes
      // as the recursive loop that runs as part of subscribe will still be running after each test
      // meaning that another test might end up processing this test's events
      const groupName = Date.now().toString();

      const workerFunction = () => {};
      await streamClient.subscribe({
        groupName, streamName, readTimeout: 20, workerFunction,
      });

      const event1 = {
        event1: 1,
      };
      const event2 = {
        event2: 2,
      };
      const events = [
        {
          eventMeta: defaultEventMeta,
          eventPayload: event1,
        },
        {
          eventMeta: defaultEventMeta,
          eventPayload: event2,
        },
      ];
      const streamClient2 = new EventStreamClient();
      await streamClient2.publishMany({ streamName, events });
      // to provide buffer for acks and getPending -
      // since acks happen in subscribe and it runs in a setTimeout loop,
      // potential for race condition, but only bc of way we assert!!!
      await sleep(500);

      // we can't spy on private functions (workerFunction is private w/in the closure in subscribe
      // but testing there are items, and they've been acked (not on PEL) works just as well
      const eventsFromStream = await getRange(streamName, 10);
      const formatted = getFormatted(eventsFromStream);
      expect(formatted[0]).to.include(`${PAYLOAD_PREFIX}-event1`, '1');
      expect(formatted[1]).to.include(`${PAYLOAD_PREFIX}-event2`, '2');

      const pending = await getPending(streamName, groupName);
      expect(pending[0]).to.equal(0);
    });

    it('should catch error if worker function fails to process item', async () => {
      // we need unique group names just for testing purposes
      // as the recursive loop that runs as part of subscribe will still be running after each test
      // meaning that another test might end up processing this test's events
      const groupName = Date.now().toString();

      const workerFunction = () => { throw Error('worker function error'); };
      await streamClient.subscribe({
        groupName, streamName, readTimeout: 20, workerFunction,
      });

      const eventPayload = {
        test: 'this is a subscribe test',
      };
      const streamClient2 = new EventStreamClient();
      await streamClient2.publish({ streamName, eventMeta: defaultEventMeta, eventPayload });
      await sleep(100);

      const pending = await getPending(streamName, groupName);
      expect(pending[0]).to.equal(1);
    });

    context('individual consumer', () => {
      // case: consumer processes its own pending items after failure,
      // BEFORE being claimed by other consumers
      it('should process pending items', async () => {
      // we need unique group names just for testing purposes
      // as the recursive loop that runs as part of subscribe will still be running after each test
      // meaning that another test might end up processing this test's events
        const groupName = Date.now().toString();

        const workerFunction = () => { throw Error('worker function error'); };
        await streamClient.subscribe({
          groupName, streamName, readTimeout: 200, workerFunction,
        });

        const eventPayload = {
          test: 'this is a subscribe test',
        };
        const streamClient2 = new EventStreamClient();
        await streamClient2.publish({ streamName, eventMeta: defaultEventMeta, eventPayload });
        await sleep(100);

        const pending = await getPending(streamName, groupName);
        expect(pending[0]).to.equal(0);
      });
    });
  });
});
