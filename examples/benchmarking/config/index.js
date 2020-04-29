module.exports = {
  master: {
    require: './firepit/master'
  },
  taskTypes: {
    sendMessage: 1
  },
  workersByType: {
    //
    // "count": defines the start worker count, will be set to "min" if fewer, or "max" if greater
    // "max": defines the max workers to run during the lifetime, "max" is priviledged on "min" if "min" > "max"
    // "min": defined the min workers to run during the lifetime
    //
    ExampleProcessor: {
      require: './firepit/processor'
    },
    ExamplePublisher: {
      require: './firepit/publisher'
    },
    ExampleMonitor: {
      require: './firepit/monitor',
      min: 1,
      max: 1
    }
  }
}
