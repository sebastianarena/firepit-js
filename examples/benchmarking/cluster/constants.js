const ClusterEvent = {
  Log: 'log',
  Exit: 'exit',
  StatusUpdate: 'status update'
}
const ClusterStatus = {
  Starting: 'starting',
  Started: 'started',
  Stopping: 'stopping',
  Stopped: 'stopped',
  Died: 'died',
  CommittedSuicide: 'committed suicide'
}
module.exports = {
  ClusterEvent: ClusterEvent,
  ClusterStatus: ClusterStatus
}
