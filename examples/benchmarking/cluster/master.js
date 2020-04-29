const { ClusterEvent, ClusterStatus } = require('./constants')
const cluster = require('cluster')
const os = require('os')
const colors = require('colors/safe')
const ClusterWorker = require('./worker')

class ClusterMaster extends ClusterWorker {
  constructor (config) {
    super(config)

    const self = this
    self.config.numCPUs = os.cpus().length
    self.config.workersByType = config.workersByType || {}
    self.config.log = config.log || {}
    self.config.log.maxEntries = parseInt(self.config.log.maxEntries || 0) || 80
    self.workersByType = {}
    self.workersTypeByPID = {}
    self.workersStatusByPID = {}
    self.workersPendingStart = 0
    self.workersPendingStop = 0

    Object.keys(self.config.workersByType).forEach(workerType => {
      self.workersByType[workerType] = {}
      if (!self.config.workersByType[workerType].max) {
        self.config.workersByType[workerType].max = self.config.numCPUs * 3 // Do not touch... this means up to 3 workers per CPU, that's even more than HyperThreading
      }
      if (!self.config.workersByType[workerType].min) {
        self.config.workersByType[workerType].min = 0
      }
      if (!self.config.workersByType[workerType].count || self.config.workersByType[workerType].count < self.config.workersByType[workerType].min) {
        self.config.workersByType[workerType].count = self.config.workersByType[workerType].min
      }
      if (self.config.workersByType[workerType].count > self.config.workersByType[workerType].max) {
        self.config.workersByType[workerType].count = self.config.workersByType[workerType].max
      }
    })

    cluster.on('exit', (worker, code, signal) => {
      self.handleStoppedWorker(worker)
    })
  }

  //
  // PRIVATE METHODS
  //

  ensureWorkers () {
    const self = this
    const toDeleteByType = {}
    const toAddByType = {}
    Object.keys(self.config.workersByType).forEach(workerType => {
      const workersPIDs = self.workersByType[workerType] && Object.keys(self.workersByType[workerType])
      const workerConfig = self.config.workersByType[workerType]
      const workersMax = workerConfig.count
      if (workersPIDs.length > workersMax) {
        toDeleteByType[workerType] = workersPIDs.length - workersMax
        self.workersPendingStop += toDeleteByType[workerType]
      } else if (workersPIDs.length < workersMax && [ClusterStatus.Started, ClusterStatus.Starting].indexOf(self.status) >= 0) {
        toAddByType[workerType] = workersMax - workersPIDs.length
        self.workersPendingStart += toAddByType[workerType]
      }
    })
    Object.keys(toDeleteByType).forEach(workerType => {
      const workersPIDs = self.workersByType[workerType] && Object.keys(self.workersByType[workerType])
      for (let i = 0; i < toDeleteByType[workerType]; i++) {
        const worker = self.workersByType[workerType][workersPIDs[i]]
        self.stopWorker(worker)
      }
    })
    Object.keys(toAddByType).forEach(workerType => {
      const workerConfig = self.config.workersByType[workerType]
      for (let i = 0; i < toAddByType[workerType]; i++) {
        self.startWorker(workerType, workerConfig.require)
      }
    })
    return Promise.resolve()
  }

  logClassColor () {
    return colors.green
  }

  addLogEntry (logEntry) {
    const self = this
    self.logEntries.push(logEntry)
    if (self.config && self.config.log && self.config.log.maxEntries) {
      const entriesToDelete = self.logEntries.length - self.config.log.maxEntries
      if (entriesToDelete) {
        self.logEntries.splice(0, entriesToDelete)
      }
    }
    return Promise.resolve(logEntry)
  }

  startWorker (workerType, workerRequire) {
    const self = this
    if (!self.exiting) {
      const env = {
        CLUSTER_WORKER_TYPE: workerType,
        CLUSTER_WORKER_REQUIRE: workerRequire
      }
      const worker = cluster.fork(env)
      self.workersTypeByPID[worker.process.pid] = workerType
      self.workersByType[workerType][worker.process.pid] = worker
      worker.on('message', event => {
        const self = this
        const worker = self.getWorkerFromEvent(event)
        if (worker) {
          self.eventReceivedFromWorker(event, worker)
        }
      })
    }
  }

  stopWorker (worker) {
    const self = this
    const event = { id: ClusterEvent.Exit }
    return self.sendEventToWorker(event, worker)
  }

  handleStoppedWorker (worker) {
    const self = this
    const workerType = self.workersTypeByPID[worker.process.pid]
    const workerStatus = self.workersStatusByPID[worker.process.pid]
    worker.removeAllListeners()
    delete self.workersByType[workerType][worker.process.pid]
    delete self.workersTypeByPID[worker.process.pid]
    delete self.workersStatusByPID[worker.process.pid]
    if (self.status === ClusterStatus.Started) {
      // It may have committed suicide, so we need to check if we need to keep going or not
      if (workerStatus === ClusterStatus.Stopped) {
        self.eventReceivedFromWorker({ id: ClusterStatus.Died }, worker)
        if (self.status === ClusterStatus.Started) {
          self.ensureWorkers()
        }
      } else {
        const workersPIDs = Object.keys(self.workersStatusByPID)
        if (!workersPIDs.length) { // All workers committed suicide
          self.stop(true)
        }
      }
    }
  }

  //
  // PRIVATE EVENT HANDLING
  //

  getWorkerFromEvent (event) {
    const self = this
    const workerType = self.workersTypeByPID[event.pid]
    if (workerType) {
      return self.workersByType[workerType][event.pid]
    }
  }

  eventReceivedFromWorker (event, worker) {
    const self = this
    if (event && event.id === ClusterEvent.Log) {
      self.addLogEntry(event.data)
    } else if (event && event.id === ClusterEvent.StatusUpdate) {
      self.workersStatusByPID[event.pid] = event.data
      if (event.data === ClusterStatus.Started) {
        self.workersPendingStart--
        if (self.status === ClusterStatus.Stopping) {
          // Added by mistake or delayed, should stop
          self.stopWorker(worker)
        }
      } else if (event.data === ClusterStatus.Stopped) {
        self.workersPendingStop--
      }
    }
  }

  starting () {
    const self = this
    return super.starting().then(_ => {
      return self.ensureWorkers()
    })
  }

  stopping (forced) {
    const self = this
    return super.stopping(forced).then(_ => {
      Object.keys(self.config.workersByType).forEach(workerType => {
        self.config.workersByType[workerType].count = 0
      })
      return self.ensureWorkers()
    }).then(_ => {
      return new Promise((resolve, reject) => {
        const waitInterval = 100
        const waitIntervalMax = 5000
        let waitedAlready = 0
        const tryStopInterval = setInterval(_ => {
          waitedAlready += waitInterval
          const pidsLeft = Object.keys(self.workersTypeByPID)
          if (waitedAlready >= waitIntervalMax) {
            self.log('stopping forcefully since childs didn\'t stop')
            process.exit()
          } else if (!pidsLeft.length) {
            clearInterval(tryStopInterval)
            resolve()
          }
        }, waitInterval)
      })
    })
  }

  //
  // PUBLIC METHODS
  //

  addWorkerForType (workerType) {
    const self = this
    if (self.status === ClusterStatus.Started) {
      if (self.config.workersByType[workerType].count < self.config.workersByType[workerType].max) {
        self.config.workersByType[workerType].count++
        return self.ensureWorkers()
      }
    }
    return Promise.resolve()
  }

  removeWorkerForType (workerType) {
    const self = this
    if (self.status === ClusterStatus.Started) {
      if (self.config.workersByType[workerType].count && self.config.workersByType[workerType].count > self.config.workersByType[workerType].min) {
        self.config.workersByType[workerType].count--
      }
      return self.ensureWorkers()
    }
    return Promise.resolve()
  }

  log (text) {
    const self = this
    return self.addLogEntry(self.prepareTextToLog(text))
  }

  sendEventToWorker (event, worker) {
    const self = this
    event.pid = process.pid
    event.pidType = self.constructor.name
    if (event && worker) {
      if (worker.isConnected()) {
        worker.send(event)
      }
    }
    return Promise.resolve()
  }

  sendEventToWorkersWithType (workerType, event) {
    const self = this
    const workerPIDs = Object.keys(self.workersByType[workerType])
    if (workerPIDs && workerPIDs.length) {
      workerPIDs.forEach(workerPID => {
        const worker = self.workersByType[workerType][workerPID]
        self.sendEventToWorker(event, worker)
      })
    }
    return Promise.resolve()
  }

  sendEventToWorkers (event) {
    const self = this
    Object.keys(self.config.workersByType).forEach(workerType => {
      self.sendEventToWorkersWithType(workerType, event)
    })
  }
}

module.exports = ClusterMaster
