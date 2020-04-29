const BaseFirepit = require('./base')

class Processor extends BaseFirepit {
  constructor () {
    super()
    const self = this
    self.firepit.subscribeToProcessTasks('throughput', 1, task => {
      console.log(`got task: ${task.id} | ${++self.tasksCount}`)
      self.firepit.finishTask(task.id)
    }).then(_ => {
      console.log('listening for new tasks')
    }).catch(_ => {
      console.log('could not subscribe to new tasks')
      self.stop()
    })
  }
}

module.exports = new Processor()
