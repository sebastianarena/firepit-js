const ExampleWorker = require('./worker')
const { ExampleEvent } = require('./constants')

class ExampleMonitor extends ExampleWorker {
  constructor (config) {
    super(config)
    const self = this
    self.nowUnit = 'milli'
    self.reset()
  }

  //
  // PRIVATE METHODS
  //
  diffWithNow (diff) {
    const self = this
    const hrTime = process.hrtime(diff)
    switch (self.nowUnit) {
      case 'milli':
        return hrTime[0] * 1000 + hrTime[1] / 1000000
      case 'micro':
        return hrTime[0] * 1000000 + hrTime[1] / 1000
      default: // 'nano'
        return hrTime[0] * 1000000000 + hrTime[1]
    }
  }

  trackNewTask (taskName, taskId, now) {
    const self = this
    if (!self.stats[taskName].whenStartedAdding) {
      self.stats[taskName].whenStartedAdding = now
    }
    self.stats[taskName].whenFinishedAdding = now
    self.stats[taskName].added++
    self.stats[taskName].pending++
    self.tasks[taskName][taskId] = now
    if (taskName === 'global') {
      self.logStats()
    }
  }

  trackStartedTask (taskName, taskId, now) {
    const self = this
    self.stats[taskName].started++
    if (taskName === 'global') {
      self.logStats()
    }
  }

  trackFinishedTask (taskName, taskId, now) {
    const self = this
    self.stats[taskName].pending--
    self.stats[taskName].finished++
    self.stats[taskName].cumulativeWaitFromAddToFinish += self.diffWithNow(self.tasks[taskName][taskId])
    self.stats[taskName].waitFromFirstAddToFinish = self.diffWithNow(self.stats[taskName].whenStartedAdding)
    self.stats[taskName].waitSinceLastAdd = self.diffWithNow(self.stats[taskName].whenFinishedAdding)
    self.stats[taskName].avgWaitFromAddToFinish = self.stats[taskName].cumulativeWaitFromAddToFinish / self.stats[taskName].finished
    self.stats[taskName].avgTimeToProcessTask = self.stats[taskName].waitFromFirstAddToFinish / self.stats[taskName].finished
    if (taskName === 'global') {
      self.checkIfFinishedAndLogStats()
    }
    delete self.tasks[taskName][taskId]
  }

  onNewTask (task) {
    const self = this

    // Checking for false starts
    if (!self.wrongByResource[task.name][task.resource]) {
      self.wrongByResource[task.name][task.resource] = []
    }
    self.wrongByResource[task.name][task.resource].push(task.id)

    // Track
    const now = process.hrtime()
    self.trackNewTask('global', task.id, now)
    self.trackNewTask(task.name, task.id, now)
  }

  onStartedTask (task) {
    const self = this

    // Checking for false starts
    if (!self.wrongByResource[task.name][task.resource]) {
      self.wrongByResource[task.name][task.resource] = []
    }
    if (self.wrongByResource[task.name][task.resource].indexOf(task.id) > 0) {
      self.stats[task.name].startedWrong++
    }

    // Track
    const now = process.hrtime()
    self.trackStartedTask('global', task.id, now)
    self.trackStartedTask(task.name, task.id, now)
  }

  onFinishedTask (task) {
    const self = this

    // Checking for false starts
    if (!self.wrongByResource[task.name][task.resource]) {
      self.wrongByResource[task.name][task.resource] = []
    }
    const resourceIndex = self.wrongByResource[task.name][task.resource].indexOf(task.id)
    if (resourceIndex >= 0) {
      self.wrongByResource[task.name][task.resource].splice(resourceIndex, 1)
    }

    // Track
    const now = process.hrtime()
    self.trackFinishedTask('global', task.id, now)
    self.trackFinishedTask(task.name, task.id, now)
  }

  checkIfFinishedAndLogStats () {
    const self = this
    const globalStats = self.stats.global
    const finished = (
      globalStats.shouldAdd &&
            globalStats.added &&
            globalStats.finished &&
            globalStats.shouldAdd === globalStats.added &&
            globalStats.added === globalStats.finished &&
            globalStats.pending === 0
    )
    self.logStats(finished)
  }

  logStats (finished) {
    const self = this
    if (!self.logStatsTimer || finished) {
      clearTimeout(self.logStatsTimer)
      self.logStatsTimer = null
      self.logStatsTimer = setTimeout(_ => {
        self.sendEventToMaster((finished) ? ExampleEvent.FinishedStats : ExampleEvent.UpdatedStats, self.stats)
        clearTimeout(self.logStatsTimer)
        self.logStatsTimer = null
        if (finished) {
          self.reset()
        }
      }, 100)
    }
  }

  resetStatsForTaskWithName (taskName) {
    const self = this
    self.stats[taskName] = {
      shouldAdd: 0,
      added: 0,
      started: 0,
      startedWrong: 0,
      pending: 0,
      finished: 0,
      cumulativeWaitFromAddToFinish: 0,
      waitFromFirstAddToFinish: 0,
      avgWaitFromAddToFinish: 0,
      avgTimeToProcessTask: 0,
      waitSinceLastAdd: 0
    }
    self.wrongByResource[taskName] = {}
    self.tasks[taskName] = {}
  }

  reset () {
    const self = this
    const taskNames = Object.keys(self.stats || {})
    self.stats = {}
    self.wrongByResource = {}
    self.tasks = {}
    self.resetStatsForTaskWithName('global')
    taskNames.forEach(taskName => {
      self.resetStatsForTaskWithName(taskName)
    })
  }

  trySetupBenchmark () {
    // Override, since the monitor takes a lot of ram to check for proper order and speed
    // We want to measure the actual memory usage for processing
  }

  //
  // PRIVATE EVENT HANDLING
  //

  eventReceivedFromMaster (event) {
    const self = this
    super.eventReceivedFromMaster(event)
    if (event && event.id === ExampleEvent.AddedTaskName) {
      self.resetStatsForTaskWithName(event.data.name)
    } else if (event && event.id === ExampleEvent.AddedTasks) {
      self.stats.global.shouldAdd = (self.stats.global.shouldAdd || 0) + event.data
      self.checkIfFinishedAndLogStats()
    }
  }
}

module.exports = ExampleMonitor
