const keypress = require('keypress')

class Base {
  constructor () {
    const self = this
    self.tasksCount = 0
    self.memoryUsage = { max: {}, last: {} }
    self.memoryCheckInterval = setInterval(_ => {
      self.memoryUsage.last = process.memoryUsage()
      Object.keys(self.memoryUsage.last).forEach(key => {
        if (!self.memoryUsage.max[key] || self.memoryUsage.max[key] < self.memoryUsage.last[key]) {
          self.memoryUsage.max[key] = self.memoryUsage.last[key]
        }
      })
    }, 200)
    keypress(process.stdin)
    process.stdin.on('keypress', self.handleKeypress.bind(self))
    process.stdin.setRawMode(true)
    process.stdin.resume()
  }

  handleKeypress (ch, key) {
    const self = this
    if ((key && key.ctrl && key.name === 'c') || (key && key.name === 'q')) {
      self.stop()
      return true
    }
    return false
  }

  stop () {
    const self = this
    clearInterval(self.memoryCheckInterval)
    self.memoryCheckInterval = null
    process.stdin.removeAllListeners('keypress')
    process.stdin.setRawMode(false)
    process.stdin.pause()
  }
}

module.exports = Base
