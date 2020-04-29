const { FirePit } = require('../../lib/firepit')
const firepit = new FirePit()
firepit.addTask('example', 'resource1', { createdAt: new Date() }).then(task => {
  console.log(`added task ${task.id}`)
}).catch(_ => {
  console.log('could not add new task')
}).then(_ => {
  return firepit.stop()
})
