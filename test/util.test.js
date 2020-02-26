import chai from 'chai'

import deepEqualInAnyOrder from 'deep-equal-in-any-order'

chai.use(deepEqualInAnyOrder)

const { expect } = chai

import { mapObjToStringArray } from '../util'

describe('util', () => {
  describe('#mapObjToStringArray', () => {
    it(`should map an object's key and value to array format`, () => {
      const converted_string = mapObjToStringArray({key: 'value'})
      expect(converted_string).to.deep.equal(['key', 'value'])
    })

    it(`should map an object's keys and values to array format for multiple keys`, () => {
      const converted_string = mapObjToStringArray({
        key1: 'value1',
        key2: 'value2'
      })
      expect(converted_string).to.deep.equal(['key1', 'value1', 'key2', 'value2'])
    })
  })
})
