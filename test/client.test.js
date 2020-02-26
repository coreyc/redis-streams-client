import chai from 'chai'

import deepEqualInAnyOrder from 'deep-equal-in-any-order'

chai.use(deepEqualInAnyOrder)

const { expect } = chai

import RedisStreamsClient from '../src'

describe('RedisStreamsClient', () => {
  describe('#deformat', () => {    
    it(`should deformat`, () => {
      const streamsClient = new RedisStreamsClient()

      const stream_data = [
        ["mystream", [
          ["1582419017278-0", ["says", "hi there"]]
        ]]
      ]

      const deformatted = streamsClient.deformat(stream_data)

      expect(deformatted).to.deep.equal([
        ["1582419017278-0", ["says", "hi there"]]
      ])
    })
  })
})
