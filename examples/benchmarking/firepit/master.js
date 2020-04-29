const { ClusterMaster, ClusterEvent, ClusterStatus } = require('../cluster')
const { ExampleEvent } = require('./constants')
const console = require('better-console')
const colors = require('colors/safe')
const keypress = require('keypress')

const randomString = function (maxLength) {
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  let text = ''
  for (var i = 0; i < maxLength; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length))
  }
  return text
}

class ExampleMaster extends ClusterMaster {
  constructor (config) {
    super(config)

    const self = this
    self.resources = 0
    self.taskTypes = []
    self.didBenchmarkSetup = false
    self.printTimer = null
    self.lastStats = null
    self.memoryUsagePerWorker = {}
    self.memoryUsageSumarized = {
      last: {},
      max: {}
    }
    self.setupKeypress()
    self.print()
    if (self.config.taskTypes) {
      if (typeof self.config.taskTypes === 'object') {
        Object.keys(self.config.taskTypes).forEach(taskName => {
          self.addTaskType(taskName, self.config.taskTypes[taskName])
        })
      } else {
        for (let i = 0; i < self.config.taskTypes; i++) {
          self.addTaskType()
        }
      }
    }
    if (self.config.resources) {
      self.addResources(self.config.resources)
    }
  }

  //
  // PRIVATE METHODS
  //

  setupKeypress () {
    const self = this
    keypress(process.stdin)
    process.stdin.on('keypress', (ch, key) => {
      if (key) {
        if ((key.ctrl && key.name === 'c') || (key.name === 'q')) {
          self.stop()
        } else if (key.name === 'w' && !self.config.benchmark) {
          self.addWorkerForType('ExamplePublisher')
        } else if (key.name === 's' && !self.config.benchmark) {
          self.removeWorkerForType('ExamplePublisher')
        } else if (key.name === 'e') {
          self.addWorkerForType('ExampleProcessor')
        } else if (key.name === 'd') {
          self.removeWorkerForType('ExampleProcessor')
        } else if (key.name === 't') {
          self.addTaskType()
        } else if (key.name === 'g') {
          self.removeTaskType()
        } else if (key.name === 'c' || key.name === 'v' || key.name === 'b' || key.name === 'n' || key.name === 'm') {
          let tasksToAdd = 0
          if (key.name === 'c') {
            tasksToAdd = 1
          } else if (key.name === 'v') {
            tasksToAdd = 10
          } else if (key.name === 'b') {
            tasksToAdd = 100
          } else if (key.name === 'n') {
            tasksToAdd = 1000
          } else if (key.name === 'm') {
            tasksToAdd = 10000
          }
          self.addTasks(tasksToAdd)
        } else if (key.name === 'y' || key.name === 'h' || key.name === 'u' || key.name === 'j' || key.name === 'i' || key.name === 'k') {
          let amountOfResources = 0
          if (key.name === 'y' || key.name === 'h') {
            amountOfResources = 1
          } else if (key.name === 'u' || key.name === 'j') {
            amountOfResources = 10
          } else if (key.name === 'i' || key.name === 'k') {
            amountOfResources = 100
          }
          if (key.name === 'h' || key.name === 'j' || key.name === 'k') {
            amountOfResources *= -1
          }
          self.addResources(amountOfResources)
        }
      }
    })
    process.stdin.setRawMode(true)
    process.stdin.resume()
  }

  printHeader () {
    const self = this
    const publishers = Object.keys(self.workersByType.ExamplePublisher)
    const processors = Object.keys(self.workersByType.ExampleProcessor)
    const monitors = Object.keys(self.workersByType.ExampleMonitor)
    console.clear()
    console.log(`${colors.cyan(processors.length)} processor${processors.length === 1 ? '' : 's'} | ${colors.cyan(publishers.length)} publisher${publishers.length === 1 ? '' : 's'} | ${colors.cyan(monitors.length)} monitor${monitors.length === 1 ? '' : 's'} | ${colors.cyan(self.taskTypes.length)} task type${self.taskTypes.length === 1 ? '' : 's'} | ${colors.cyan(self.resources)} resource${self.resources === 1 ? '' : 's'}`)
    console.log()
    if (!self.config.benchmark) {
      console.log(`${colors.cyan('w/s')}           : add / remove publishers`)
    }
    console.log(`${colors.cyan('e/d')}           : add / remove processors`)
    console.log(`${colors.cyan('t/g')}           : add / remove task types`)
    console.log(`${colors.cyan('y/h, u/j, i/k')} : add / remove 1 / 10 / 100 resources`)
    console.log(`${colors.cyan('c/v/b/n/m')}     : add 1 / 10 / 100 / 1000 / 10000 tasks per publisher`)
    console.log(`${colors.cyan('q/ctrl+c')}      : quit`)
    console.log()
  }

  print () {
    const self = this
    if (!process.env.DEBUG && !self.printTimer) {
      self.printTimer = setTimeout(_ => {
        clearTimeout(self.printTimer)
        self.printTimer = null
        self.printHeader()
        if (self.logEntries.length) {
          self.logEntries.forEach(logEntry => {
            console.log(logEntry)
          })
        }
        console.log()
        if (self.config.benchmark && self.lastStats) {
          self.memoryUsageSumarized.last = process.memoryUsage()
          Object.keys(self.memoryUsagePerWorker).forEach(workerPID => {
            const memoryUsageWorker = self.memoryUsagePerWorker[workerPID].last
            if (memoryUsageWorker) {
              self.memoryUsageSumarized.last.heapUsed += memoryUsageWorker.heapUsed || 0
              self.memoryUsageSumarized.last.heapTotal += memoryUsageWorker.heapTotal || 0
              self.memoryUsageSumarized.last.rss += memoryUsageWorker.rss || 0
            }
          })
          Object.keys(self.memoryUsageSumarized.last).forEach(key => {
            if (!self.memoryUsageSumarized.max[key] || self.memoryUsageSumarized.max[key] < self.memoryUsageSumarized.last[key]) {
              self.memoryUsageSumarized.max[key] = self.memoryUsageSumarized.last[key]
            }
          })
          const bytesToMb = 1048576
          if (self.lastStats.global) {
            console.log(`${colors.cyan(`${self.lastStats.global.added}`)} added tasks`)
            console.log(`${colors.cyan(`${self.lastStats.global.pending} / ${self.lastStats.global.added}`)} pending tasks`)
            console.log(`${colors.cyan(`${self.lastStats.global.startedWrong}`)} started out of order tasks`)
            console.log()
            console.log(`${colors.cyan(`${(self.lastStats.global.cumulativeWaitFromAddToFinish / 1000)}s`)} cumulative wait time for all tasks (from add to finish)`)
            console.log(`${colors.cyan(`${(self.lastStats.global.waitFromFirstAddToFinish)}ms`)} wait time (from first task to last finished task)`)
            console.log(`${colors.cyan(`${(self.lastStats.global.avgWaitFromAddToFinish)}ms`)} avg. wait time per task (from add to finish)`)
            console.log(`${colors.cyan(`${(self.lastStats.global.avgTimeToProcessTask)}ms`)} avg. process time per task`)
            console.log(`${colors.cyan(`${(self.lastStats.global.waitSinceLastAdd)}ms`)} wait time since last task added`)
            console.log()
            console.log(`${colors.cyan(`${(self.memoryUsageSumarized.max.heapUsed / bytesToMb).toFixed(5)}mb / ${(self.memoryUsageSumarized.max.heapTotal / bytesToMb).toFixed(2)}mb / ${(self.memoryUsageSumarized.max.rss / bytesToMb).toFixed(2)}mb`)} heap used / heap total / resident, max`)
            console.log(`${colors.cyan(`${(self.memoryUsageSumarized.last.heapUsed / bytesToMb).toFixed(5)}mb / ${(self.memoryUsageSumarized.last.heapTotal / bytesToMb).toFixed(2)}mb / ${(self.memoryUsageSumarized.last.rss / bytesToMb).toFixed(2)}mb`)} heap used / heap total / resident, last`)
          }
        }
      }, 100)
    }
  }

  addLogEntry (logEntry) {
    const self = this
    super.addLogEntry(logEntry)
    if (process.env.DEBUG) {
      console.log(logEntry)
    } else {
      self.print()
    }
  }

  addTaskType (taskName, taskMaxPerWorker) {
    const self = this
    if (!taskName) {
      taskName = randomString(32)
    }
    const taskType = {
      name: taskName,
      max: taskMaxPerWorker || 1
    }
    const event = {
      id: ExampleEvent.AddedTaskName,
      data: taskType
    }
    self.taskTypes.push(taskType)
    self.log(`adding task type: ${taskName}`)
    self.sendEventToWorkers(event)
  }

  removeTaskType () {
    const self = this
    if (self.taskTypes.length) {
      const taskType = self.taskTypes[0]
      self.log(`removing task type: ${taskType.name}`)
      self.taskTypes.splice(0, 1)
      const event = {
        id: ExampleEvent.RemovedTaskName,
        data: taskType
      }
      self.sendEventToWorkers(event)
    }
  }

  addTasks (tasksToAdd) {
    const self = this
    const publishers = Object.keys(self.workersByType.ExamplePublisher)
    if (publishers && publishers.length && self.taskTypes && self.taskTypes.length && self.resources) {
      const event = {
        id: ExampleEvent.ShouldAddTasks,
        data: {
          resources: self.resources,
          tasks: tasksToAdd
        }
      }
      self.log(`adding ${tasksToAdd} tasks per publisher (${tasksToAdd * publishers.length} total)`)
      self.sendEventToWorkers(event)
    } else {
      self.log(`cannot add tasks without publishers (${publishers.length}), task types (${self.taskTypes.length}) and resources (${self.resources})`)
    }
  }

  addResources (amountOfResources) {
    const self = this
    self.resources += amountOfResources
    if (self.resources < 0) {
      self.resources = 0
    }
    self.log(`${(amountOfResources < 0) ? 'removing' : 'adding'} ${Math.abs(amountOfResources)} resources`)
  }

  trySetupBenchmark () {
    const self = this
    if (self.config.benchmark && !self.didBenchmarkSetup && self.workersPendingStart === 0) {
      self.didBenchmarkSetup = true
      if (self.config.tasks) {
        self.addTasks(self.config.tasks)
      }
      return Promise.resolve(true)
    }
    return Promise.resolve(false)
  }

  //
  // PRIVATE EVENT HANDLING
  //

  eventReceivedFromWorker (event, worker) {
    const self = this
    super.eventReceivedFromWorker(event, worker)
    if (event && event.id === ClusterEvent.StatusUpdate && event.data === ClusterStatus.Started) {
      self.taskTypes.forEach(taskType => {
        const event = {
          id: ExampleEvent.AddedTaskName,
          data: taskType
        }
        self.sendEventToWorker(event, worker)
      })
      self.trySetupBenchmark()
    } else if (event && (event.id === ExampleEvent.UpdatedStats || event.id === ExampleEvent.FinishedStats)) {
      self.lastStats = event.data
      if (event.id === ExampleEvent.FinishedStats && self.config.benchmark === 'final') {
        console.log(JSON.stringify(event.data, null, 4))
      }
      if (event.id === ExampleEvent.FinishedStats) {
        self.log(`stats: ${JSON.stringify(event.data.global)}`)
        if (self.config.benchmark) {
          self.stop()
        }
      }
    } else if (event && event.id === ExampleEvent.AddedTasks) {
      // Broadcast
      self.log(`added ${event.data} tasks in ${event.pidType}(${event.pid})`)
      self.sendEventToWorkers(event)
    } else if (event && event.id === ExampleEvent.MemoryUsage) {
      self.memoryUsagePerWorker[event.pid] = event.data
    }
    self.print()
  }

  stopping (forced) {
    process.stdin.removeAllListeners('keypress')
    process.stdin.setRawMode(false)
    process.stdin.pause()
    return super.stopping(forced)
  }
}

module.exports = ExampleMaster
