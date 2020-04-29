const cluster = require('cluster')
const configFile = process.env.CONFIG || './config'
const config = require(configFile)
const processes = []
if (cluster.isMaster) {
  const MasterClass = require(config.master.require)
  processes.push(new MasterClass(config))
} else {
  const WorkerClass = require(process.env.CLUSTER_WORKER_REQUIRE)
  processes.push(new WorkerClass(config))
}
