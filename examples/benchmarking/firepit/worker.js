const { ClusterWorker, ClusterStatus } = require('../cluster')
const { FirePit, FirePitStatus, FirePitTaskEvent, FirePitEvent } = require('../../../lib/firepit')
const { ExampleEvent } = require('./constants')
const colors = require('colors/safe') // eslint-disable-line no-unused-vars

class ExampleWorker extends ClusterWorker {
  constructor (config) {
    super(config)
    const self = this
    self.memoryUsage = {
      max: {},
      last: {}
    }
    self.firepit = null
  }

  subscribeToTaskType (taskType) {
    const self = this
    const subscriptions = {}
    if (self.onNewTask) {
      subscriptions[FirePitTaskEvent.New] = self.onNewTask.bind(self)
    }
    if (self.onStartedTask) {
      subscriptions[FirePitTaskEvent.Started] = self.onStartedTask.bind(self)
    }
    if (self.onUpdatedTask) {
      subscriptions[FirePitTaskEvent.Updated] = self.onUpdatedTask.bind(self)
    }
    if (self.onFinishedTask) {
      subscriptions[FirePitTaskEvent.Finished] = self.onFinishedTask.bind(self)
    }
    if (self.onProcessTask) {
      subscriptions[FirePitTaskEvent.Process] = self.onProcessTask.bind(self)
    }
    const subscriptionsTypes = Object.keys(subscriptions)
    if (self.firepit && subscriptionsTypes.length) {
      return self.firepit.subscribeToTasks(taskType.name, taskType.max, subscriptions).then(_ => {
        self.log(`subscribed to ${taskType.name} for ${subscriptionsTypes.join(', ')}`)
        return Promise.resolve()
      }).catch(error => {
        if (error) {
          self.log(`could not subscribe to ${taskType.name}: ${(error.stack) ? error.stack : error.toString()}`)
        }
        return Promise.resolve()
      })
    }
    return Promise.resolve()
  }

  unsubscribeToTaskType (taskType) {
    const self = this
    if (self.firepit && self.firepit.isSubscribedToTasks(taskType.name)) {
      return self.firepit.unsubscribeToTasks(taskType.name).then(_ => {
        self.log(`unsubscribed to ${taskType.name}`)
        return Promise.resolve()
      }).catch(error => {
        if (error) {
          self.log(`could not unsubscribe to ${taskType.name}: ${(error.stack) ? error.stack : error.toString()}`)
        }
        return Promise.resolve()
      })
    }
    return Promise.resolve()
  }

  trySetupBenchmark () {
    const self = this
    if (self.config.benchmark && !self.didBenchmarkSetup) {
      self.didBenchmarkSetup = true
      self.memoryCheckInterval = setInterval(_ => {
        self.memoryUsage.last = process.memoryUsage()
        Object.keys(self.memoryUsage.last).forEach(key => {
          if (!self.memoryUsage.max[key] || self.memoryUsage.max[key] < self.memoryUsage.last[key]) {
            self.memoryUsage.max[key] = self.memoryUsage.last[key]
          }
        })
        self.sendEventToMaster(ExampleEvent.MemoryUsage, self.memoryUsage)
      }, 100)
      return Promise.resolve(true)
    }
    return Promise.resolve(false)
  }

  //
  // PRIVATE EVENT HANDLING
  //

  eventReceivedFromMaster (event) {
    const self = this
    super.eventReceivedFromMaster(event)
    if (event && event.id === ExampleEvent.AddedTaskName) {
      self.subscribeToTaskType(event.data)
    } else if (event && event.id === ExampleEvent.RemovedTaskName) {
      self.unsubscribeToTaskType(event.data)
    }
  }

  statusUpdateReceivedFromFirePit (status) {
    const self = this
    if (status === FirePitStatus.Stopped && [ClusterStatus.Started, ClusterStatus.Starting].indexOf(self.status) >= 0) {
      self.stop(true)
    }
  }

  logReceivedFromFirePit (text) {
    // const self = this
    // self.log(`(${colors.magenta('firepit')}) ${text}`)
  }

  starting () {
    const self = this
    return super.starting().then(_ => {
      self.firepit = new FirePit(self.config.firepit)
      self.firepit.on(FirePitEvent.StatusUpdate, self.statusUpdateReceivedFromFirePit.bind(self))
      self.firepit.on(FirePitEvent.Log, self.logReceivedFromFirePit.bind(self))
      return self.firepit.start()
    })
  }

  started () {
    const self = this
    return super.started().then(_ => {
      return self.trySetupBenchmark()
    })
  }

  stopping (forced) {
    const self = this
    return super.stopping(forced).then(_ => {
      if (self.firepit) {
        return self.firepit.stop()
      }
      return Promise.resolve()
    }).then(_ => {
      if (self.firepit) {
        self.firepit.removeAllListeners()
        self.firepit = null
      }
      if (self.memoryCheckInterval) {
        clearInterval(self.memoryCheckInterval)
        self.memoryCheckInterval = null
      }
      return Promise.resolve()
    })
  }
}

module.exports = ExampleWorker
