export const mapObjToStringArray = obj => {
  if (!obj || typeof obj !== 'object') throw new Error('must pass in object')
  
  const arr = []

  for (const key in obj) {
    arr.push(key, obj[key])
  }

  return arr
}