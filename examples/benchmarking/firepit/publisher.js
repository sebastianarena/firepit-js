const ExampleWorker = require('./worker')
const { ExampleEvent } = require('./constants')
const _ = require('lodash')

class ExamplePublisher extends ExampleWorker {
  constructor (config) {
    super(config)
    const self = this
    self.subscribedToTasksNames = []
  }

  //
  // PRIVATE METHODS
  //

  randomTask () {
    const self = this
    return (self.subscribedToTasksNames && self.subscribedToTasksNames.length && self.subscribedToTasksNames[_.random(0, self.subscribedToTasksNames.length - 1)]) || null
  }

  randomResource (resourcesCount) {
    return 'resource-' + _.pad(_.random(1, resourcesCount), resourcesCount.toString().length, '0').toUpperCase()
  }

  trySetupBenchmark () {
    // Override, since the publisher takes minimum ram, but it is held there until all processors finish
    // We want to measure the actual memory usage for PROCESSING, not publishing
  }

  //
  // PRIVATE EVENT HANDLING
  //

  eventReceivedFromMaster (event) {
    const self = this
    super.eventReceivedFromMaster(event)
    if (event && event.id === ExampleEvent.ShouldAddTasks && event.data && event.data.resources && event.data.tasks) {
      self.addTasks(event.data.resources, event.data.tasks)
    } else if (event && event.id === ExampleEvent.AddedTaskName) {
      self.subscribedToTasksNames.push(event.data.name)
    } else if (event && event.id === ExampleEvent.RemovedTaskName) {
      const i = self.subscribedToTasksNames.indexOf(event.data.name)
      if (i >= 0) {
        self.subscribedToTasksNames.splice(i, 1)
      }
    }
  }

  //
  // PUBLIC METHODS
  //

  addTaskBundle (resourcesCount, tasksCount) {
    const self = this
    const tasksPromiseBundle = []
    for (let i = 0; i < tasksCount; i++) {
      const randomTask = self.randomTask()
      const randomResource = self.randomResource(resourcesCount)
      if (self.firepit && randomTask && randomResource) {
        tasksPromiseBundle.push(self.firepit.addTask(randomTask, randomResource, { createdAt: new Date() }))
      }
    }
    if (tasksPromiseBundle && tasksPromiseBundle.length) {
      return Promise.all(tasksPromiseBundle)
    } else {
      return Promise.resolve()
    }
  }

  addTasks (resourcesCount, tasksCount) {
    const self = this
    let promise = Promise.resolve()
    const tasksBundlesCount = 50
    const tasksBundlesToAdd = parseInt(tasksCount / tasksBundlesCount)
    for (let i = 0; i < tasksBundlesToAdd; i++) {
      promise = promise.then(_ => {
        return self.addTaskBundle(resourcesCount, tasksBundlesCount)
      })
    }
    return promise.then(_ => {
      return self.addTaskBundle(resourcesCount, tasksCount - (tasksBundlesToAdd * tasksBundlesCount))
    }).then(_ => {
      return self.sendEventToMaster(ExampleEvent.AddedTasks, tasksCount)
    })
  }
}

module.exports = ExamplePublisher
