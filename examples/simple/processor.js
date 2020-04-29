const { FirePit } = require('../../lib/firepit')
const firepit = new FirePit()
firepit.subscribeToProcessTasks('example', 1, task => {
  console.log(`got task: ${task.id}`)
  firepit.finishTask(task.id)
}).then(_ => {
  console.log('listening for new tasks')
}).catch(_ => {
  console.log('could not subscribe to new tasks')
  return firepit.stop()
})
