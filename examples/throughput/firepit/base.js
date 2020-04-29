const { FirePit, FirePitEvent } = require('../../../lib/firepit')
const Base = require('../base')

class BaseFirepit extends Base {
  constructor () {
    super()
    const self = this
    self.firepit = new FirePit({
      // redis: {
      //     // sentinels: [
      //     //     {
      //     //         host: '10.0.100.104',
      //     //         port: 26379
      //     //     },
      //     //     {
      //     //         host: '10.0.100.113',
      //     //         port: 26379
      //     //     },
      //     //     {
      //     //         host: '10.0.100.113',
      //     //         port: 26380
      //     //     }
      //     // ],
      //     // name: 'mymaster'
      //     // host: '10.0.100.104',
      //     // port: 6379
      // }
    })
    // self.firepit.on(FirePitEvent.StatusUpdate, (status) => {
    //     console.log(`-- firepit status: ${status}`)
    // })
    // self.firepit.on(FirePitEvent.Log, (log) => {
    //     console.log(`-- firepit: ${log}`)
    // })
  }

  stop () {
    const self = this
    super.stop()
    if (self.firepit) {
      self.firepit.stop()
      self.firepit = null
    }
  }
}

module.exports = BaseFirepit
