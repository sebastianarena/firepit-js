const BaseFirepit = require('./base')
const _ = require('lodash')

class Publisher extends BaseFirepit {
  constructor () {
    super()
    const self = this
    self.resourcesCount = 1000 // How many resources the tasks have at their disposal
    self.publishTimerInterval = 1000 // How often should we publish tasks
    self.publishTasksCount = 110 // Tasks to add in each publishTimerInterval
    self.tasksBundlesCount = 100 // Max tasks to add in one single Promise
    self.publishTimer = setInterval(self.addTasks.bind(self), self.publishTimerInterval)
  }

  handleKeypress (ch, key) {
    const self = this
    if (!super.handleKeypress(ch, key)) {
      if (ch === '+') {
        self.publishTasksCount += 10
        console.log(`new tasks count per interval: ${self.publishTasksCount}`)
      } else if (ch === '-') {
        self.publishTasksCount -= 10
        if (self.publishTasksCount < 0) {
          self.publishTasksCount = 0
        }
        console.log(`new tasks count per interval: ${self.publishTasksCount}`)
      }
    }
  }

  stop () {
    const self = this
    clearInterval(self.publishTimer)
    self.publishTimer = null
    super.stop()
  }

  getRandomTask () {
    return 'throughput'
  }

  getRandomResource () {
    const self = this
    return 'resource-' + _.pad(_.random(1, self.resourcesCount), self.resourcesCount.toString().length, '0').toUpperCase()
  }

  addTaskBundle (tasksCount) {
    const self = this
    const tasksPromiseBundle = []
    for (let i = 0; i < tasksCount; i++) {
      const randomTask = self.getRandomTask()
      const randomResource = self.getRandomResource()
      if (self.firepit && randomTask && randomResource) {
        tasksPromiseBundle.push(self.firepit.addTask(randomTask, randomResource, { createdAt: new Date() }))
        self.tasksCount++
      }
    }
    if (tasksPromiseBundle && tasksPromiseBundle.length) {
      return Promise.all(tasksPromiseBundle)
    } else {
      return Promise.resolve()
    }
  }

  addTasks (tasksCount) {
    const self = this
    let promise = Promise.resolve()
    const tasksBundlesToAdd = parseInt(self.publishTasksCount / self.tasksBundlesCount)
    for (let i = 0; i < tasksBundlesToAdd; i++) {
      promise = promise.then(_ => {
        return self.addTaskBundle(self.tasksBundlesCount)
      })
    }
    return promise.then(_ => {
      return self.addTaskBundle(self.publishTasksCount - (tasksBundlesToAdd * self.tasksBundlesCount))
    }).then(_ => {
      console.log(`${new Date().toISOString()} - added ${self.publishTasksCount} tasks | ${self.tasksCount}`)
    })
  }
}

module.exports = new Publisher()
