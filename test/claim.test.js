import chai from 'chai'

import deepEqualInAnyOrder from 'deep-equal-in-any-order'

chai.use(deepEqualInAnyOrder)

const { expect } = chai

import RedisStreamsClaimer from '../src/claim'

describe('RedisStreamsClaimer', () => {
  describe('#handlePending', () => {    
    it(`should work`, async () => {
      const streamsClaimer = new RedisStreamsClaimer()

      await streamsClaimer.handlePending('test_stream', 'test_group')

      // expect consumer process 2 to have claimed the dead/timed out process items
    })
  })
})
