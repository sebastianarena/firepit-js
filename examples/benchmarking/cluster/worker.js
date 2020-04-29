const { ClusterEvent, ClusterStatus } = require('./constants')
const colors = require('colors/safe')

class ClusterWorker {
  constructor (config) {
    const self = this
    self.config = config || {}
    self.status = ClusterStatus.Stopped
    self.logEntries = []
    self.start()
  }

  //
  // PRIVATE METHODS
  //

  updateStatus (newStatus) {
    const self = this
    self.status = newStatus
    self.log(newStatus)
    self.sendEventToMaster(ClusterEvent.StatusUpdate, newStatus)
    return Promise.resolve()
  }

  sendEventToMaster (eventType, eventData) {
    const self = this
    if (process.send) {
      // Only workers have .send, master does not have it
      try {
        process.send({
          id: eventType,
          pid: process.pid,
          pidType: self.constructor.name,
          data: eventData
        })
      } catch (_) {
      }
    }
    return Promise.resolve()
  }

  logHeader () {
    return colors.cyan(new Date().toISOString())
  }

  logClassColor () {
    return colors.yellow
  }

  logClass () {
    const self = this
    const logClassColor = self.logClassColor()
    return logClassColor(`${self.constructor.name}(${process.pid})`)
  }

  prepareTextToLog (text) {
    const self = this
    return `${self.logHeader()} ${self.logClass()} ${text}`
  }

  //
  // PRIVATE EVENT HANDLING
  //

  eventReceivedFromMaster (event) {
    const self = this
    if (event && event.id === ClusterEvent.Exit) {
      self.stop()
    }
  }

  starting () {
    const self = this
    if (self.status === ClusterStatus.Stopped) {
      process.on('message', self.eventReceivedFromMaster.bind(self))
      return self.updateStatus(ClusterStatus.Starting)
    }
    return Promise.reject(new Error('not stopped'))
  }

  started () {
    const self = this
    if (self.status === ClusterStatus.Starting) {
      return self.updateStatus(ClusterStatus.Started)
    }
    return Promise.reject(new Error('not starting'))
  }

  start () {
    const self = this
    return self.starting().then(_ => {
      return self.started()
    }).catch(error => {
      if (error) {
        self.log(`failed to start ${self.constructor.name}: ${(error.stack) ? error.stack : error.toString()}`)
      }
      return self.stop(true).then(_ => {
        return Promise.reject(error)
      })
    })
  }

  stopping (forced) {
    const self = this
    if ([ClusterStatus.Starting, ClusterStatus.Started].indexOf(self.status) >= 0) {
      process.removeAllListeners()
      return self.updateStatus(ClusterStatus.Stopping)
    }
    return Promise.reject(new Error('not starting/started'))
  }

  stopped (forced) {
    const self = this
    if (self.status === ClusterStatus.Stopping) {
      const status = (forced === true) ? ClusterStatus.CommittedSuicide : ClusterStatus.Stopped
      return self.updateStatus(status)
    }
    return Promise.reject(new Error('not stopping'))
  }

  //
  // PUBLIC METHODS
  //

  stop (forced) {
    const self = this
    return self.stopping(forced).then(_ => {
      return self.stopped(forced)
    }).catch(error => {
      if (error) {
        self.log(`failed to stop ${self.constructor.name}: ${(error.stack) ? error.stack : error.toString()}`)
      }
      return Promise.reject(error)
    })
  }

  log (text) {
    const self = this
    return self.sendEventToMaster(ClusterEvent.Log, self.prepareTextToLog(text))
  }
}

module.exports = ClusterWorker
