module.exports = {
  master: {
    require: './firepit/master'
  },
  benchmark: 'realtime', // 'realtime' or 'final' (to only show the last stats only)
  tasks: 1000,
  taskTypes: {
    // 'receiveMessage': 1,
    sendMessage: 10
  },
  // taskTypes: 1,
  resources: 10000,
  workersByType: {
    //
    // "count": defines the start worker count, will be set to "min" if fewer, or "max" if greater
    // "max": defines the max workers to run during the lifetime, "max" is priviledged on "min" if "min" > "max"
    // "min": defined the min workers to run during the lifetime
    //
    ExampleProcessor: {
      require: './firepit/processor',
      count: 1
    },
    ExamplePublisher: {
      require: './firepit/publisher',
      count: 10
    },
    ExampleMonitor: {
      require: './firepit/monitor',
      min: 1,
      max: 1
    }
  }
}
