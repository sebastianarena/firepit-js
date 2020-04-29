const ExampleWorker = require('./worker')
const _ = require('lodash') // eslint-disable-line no-unused-vars

class ExampleProcessor extends ExampleWorker {
  //
  // PRIVATE METHODS
  //

  // Process with a random delay
  // onProcessTask(task, taskStarted, taskUpdated, taskFinished) {
  //     const self = this
  //     self.log(`doing ${task.id}`)
  //     taskStarted()
  //     setTimeout(_ => {
  //         self.log(`done ${task.id}`)
  //         taskFinished()
  //     }, _.random(0, 1000))
  // }

  // Process right away
  onProcessTask (task, taskStarted, taskUpdated, taskFinished) {
    const self = this
    self.log(`done ${task.id}`)
    taskFinished()
  }
}

module.exports = ExampleProcessor
