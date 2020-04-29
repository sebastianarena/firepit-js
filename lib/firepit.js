const Redis = require('ioredis')
const { EventEmitter } = require('events')

const FirePitStatus = {
  Starting: 'starting',
  Started: 'started',
  Stopping: 'stopping',
  Stopped: 'stopped'
}

const FirePitEvent = {
  Log: 'log',
  StatusUpdate: 'status update'
}

const FirePitTaskEvent = {
  New: 'new',
  Started: 'started',
  Updated: 'updated',
  Finished: 'finished',
  Process: 'process'
}

const randomString = function (maxLength) {
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  let text = ''
  for (var i = 0; i < maxLength; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length))
  }
  return text
}

class FirePit extends EventEmitter {
  //
  // `config` is an optional object with the following fields
  //
  //      namespaceSeparator (optional string, default: ':')          | to create the string projectname:tasktype:resouce:taskid
  //      projectName (optional string, default: 'firepit')           | the name that will be appended to the keys in Redis, to not overlap with other Redis keys
  //      dequeueTTL (optional integer, default: 5)                   | in seconds, then minimum amount of seconds interval to not waste CPU
  //      gracefulStopWait (optional integer, default/min: 5000)  | in milliseconds, then minimum amount of milliseconds to wait to shutdown during a graceful stop
  //      redis (optional object)                                     | Redis' configuration
  //          host (optional integer, default: 6379)                  | Redis' server current port
  //          port (optional string, default: 'localhost')            | Redis' server current ip or domain name
  //
  // TODO redisClusterConnection
  // TODO redisClusterOptions
  // TODO redis.sentinels

  constructor (config) {
    super()

    const self = this
    self.config = config || {}
    self.config.namespaceSeparator = self.config.namespaceSeparator || ':'
    self.config.projectName = self.config.projectName || 'firepit'
    self.config.dequeueTTL = Math.max(parseInt(self.config.dequeueTTL || 0), 5)
    self.config.gracefulStartWait = Math.max(parseInt(self.config.gracefulStopWait || 0), 5000)
    self.config.gracefulStopWait = Math.max(parseInt(self.config.gracefulStopWait || 0), 5000)
    self.config.redis = self.config.redis || {}
    if (!self.config.redis.sentinels) {
      self.config.redis.host = self.config.redis.host || 'localhost'
      self.config.redis.port = self.config.redis.port || 6379
    }
    self.config.redis.lazyConnect = true // connect on demand when asked to
    self.config.redis.retryStrategy = self.config.redis.retryStrategy || function (times) {
      if (self.status === FirePitStatus.Started && times === 1) {
        self.log('reconnecting...')
      }
      if (times >= 20) {
        if (self.status === FirePitStatus.Started) {
          self.stop(true) // Force shutdown
        }
        return null
      } else {
        return Math.min(times * 50, 2000)
      }
    }

    self.config.autostart = typeof self.config.autostart === 'undefined' ? true : (self.config.autostart === true)
    self.id = randomString(32)
    self.status = FirePitStatus.Stopped
    self.initializeRedisClients()
    if (self.config.autostart) {
      setTimeout(() => {
        self.doStart()
      }, 5) // Wait 5ms to auto-start to take place to allow proper construction and "on" methods called
    }
  }

  // PRIVATE METHODS

  updateStatus (newStatus) {
    const self = this
    if (self.status !== newStatus) {
      self.status = newStatus
      self.emit(FirePitEvent.StatusUpdate, newStatus)
      return self.log(newStatus)
    }
    return Promise.resolve()
  }

  log (text) {
    const self = this
    self.emit(FirePitEvent.Log, text)
    return Promise.resolve()
  }

  getPendingTasksForType (typeKey, typeKeyPendingMax) {
    const self = this
    let resourceKey = null
    let dataKey = null

    // Check the 'ready queue' for this task name, this is a blocking op
    self.dequeueResourceForTypeInReady(typeKey).then(gotResourceKey => {
      if (gotResourceKey) {
        resourceKey = gotResourceKey
        return self.dequeueTaskForResource(resourceKey)
      }
      return Promise.resolve()
    }).then(gotDataKey => {
      if (gotDataKey) {
        dataKey = gotDataKey
        // Get the content of the task itself
        return self.getData(dataKey)
      } else {
        // Remove from blocked
        return self.redisTransactions.lrem(self.getNameForBlockedList(typeKey), 1, resourceKey).then(_ => {
          // Remove resource from all set, we are done!
          return self.redisTransactions.multi()
            .hdel(self.getNameForAllSet(typeKey), resourceKey)
            .exec().then(_ => {
              return Promise.reject(new Error('Controlled fail')) // got to silent fail
            })
        })
      }
    }).then(data => {
      if (data && self.subscriptions && self.subscriptions[typeKey] && self.subscriptions[typeKey][FirePitTaskEvent.Process]) {
        const message = {
          id: dataKey,
          type: typeKey.substring(self.config.projectName.length + 1),
          typeKey: typeKey,
          resource: resourceKey.substring(self.config.projectName.length + 1),
          resourceKey: resourceKey,
          data: data
        }
        self.subscriptions[typeKey][FirePitTaskEvent.Process](
          message,
          self.startTask.bind(self, dataKey),
          self.updateTask.bind(self, dataKey),
          self.finishTask.bind(self, dataKey)
        )
      }
      return Promise.resolve(false)
    }).catch(_ => {
      // Fail quietly, allow this pending check token to be re-added
      return Promise.resolve(true)
    }).then(forcedGetPendingTasks => {
      if ([FirePitStatus.Starting, FirePitStatus.Started].indexOf(self.status) >= 0) {
        if (forcedGetPendingTasks) {
          return self.getPendingTasksForType(typeKey)
        } else if (typeKeyPendingMax) {
          return self.getPendingTasksForType(typeKey, typeKeyPendingMax - 1)
        }
      }
      return Promise.resolve()
    })

    // Always return, because it's a blocking task
    return Promise.resolve()
  }

  eventsIsValid (events) {
    let isValid = true
    const eventsKeys = Object.keys(events)
    const firePitEventKeys = Object.keys(FirePitTaskEvent)
    for (let i = 0; i < eventsKeys.length; i++) {
      let eventFound = false
      for (let j = 0; j < firePitEventKeys.length; j++) {
        if (FirePitTaskEvent[firePitEventKeys[j]] === eventsKeys[i]) {
          eventFound = true
        }
      }
      isValid = isValid && eventFound && (typeof events[eventsKeys[i]] === 'function')
    }
    return isValid
  }

  isSubscribedToTasks (type) {
    const self = this
    const typeKey = self.config.projectName + self.config.namespaceSeparator + type
    return self.subscriptions[typeKey] && Object.keys(self.subscriptions[typeKey])
  }

  createRedisClient () {
    const self = this
    if (self.config.redisClusterConnection && self.config.redisClusterOptions) {
      return new Redis.Cluster(self.config.redisClusterConnection, self.config.redisClusterOptions)
    } else {
      return new Redis(self.config.redis)
    }
  }

  initializeRedisClients () {
    const self = this
    self.subscriptions = {}
    self.subscriptionsMaxTasksAllowed = {}
    self.subscriptionsClients = {}

    // Client creation and connection is important
    // As first tasks are queued and triggered upon connection
    // If clients are not started in order, we might not get info correct when coming online
    // Do not clone or duplicate clients, create their own independent connections to the server

    // 1st client, to handle simple operations
    if (!self.redisClient) {
      self.redisClient = self.createRedisClient()
    }
    self.redisClient.on(['error', 'sentinelError'], self.onRedisError.bind(self, 'redisClient'))

    // 2nd client, to subscribe to notifications
    if (!self.redisSubscriber) {
      self.redisSubscriber = self.createRedisClient()
    }
    self.redisSubscriber.on('error', self.onRedisError.bind(self, 'redisSubscriber'))
    self.redisSubscriber.on('message', self.onRedisMessage.bind(self))

    // 3rd client, to publish notifications
    if (!self.redisPublisher) {
      self.redisPublisher = self.createRedisClient()
    }
    self.redisPublisher.on('error', self.onRedisError.bind(self, 'redisPublisher'))

    // 4th client, to handle adding tasks specifically via transactions (only op for the moment)
    if (!self.redisTransactions) {
      self.redisTransactions = self.createRedisClient()
    }
    self.redisTransactions.on('error', self.onRedisError.bind(self, 'redisTransactions'))

    return Promise.resolve()
  }

  resetStartTimer () {
    const self = this
    if (self.startTimer) {
      clearInterval(self.startTimer)
      self.startTimer = null
      self.startPromise = null
    }
    return Promise.resolve()
  }

  doStart () {
    const self = this
    if ([FirePitStatus.Stopped, FirePitStatus.Starting].indexOf(self.status) >= 0) {
      if (!self.startPromise) {
        self.startPromise = self.updateStatus(FirePitStatus.Starting).then(_ => {
          return self.redisClient.connect()
        }).catch(_ => {
          // Ignore promises from this connects, everything is handled by onRedisError
        }).then(_ => {
          return self.redisSubscriber.connect()
        }).catch(_ => {
          // Ignore promises from this connects, everything is handled by onRedisError
        }).then(_ => {
          return self.redisPublisher.connect()
        }).catch(_ => {
          // Ignore promises from this connects, everything is handled by onRedisError
        }).then(_ => {
          return self.redisTransactions.connect()
        }).catch(_ => {
          // Ignore promises from this connects, everything is handled by onRedisError
        }).then(_ => {
          return new Promise((resolve, reject) => {
            const waitInterval = 10
            self.startTimer = setInterval(_ => {
              if (
                self.redisClient.status === 'ready' &&
                self.redisSubscriber.status === 'ready' &&
                self.redisPublisher.status === 'ready' &&
                self.redisTransactions.status === 'ready'
              ) {
                self.resetStartTimer()
                resolve()
              } else if (
                self.redisClient.status === 'end' &&
                self.redisSubscriber.status === 'end' &&
                self.redisPublisher.status === 'end' &&
                self.redisTransactions.status === 'end'
              ) {
                self.resetStartTimer()
                reject(new Error('Controlled fail'))
              }
            }, waitInterval)
          })
        }).then(_ => {
          return self.updateStatus(FirePitStatus.Started)
        }).catch(_ => {
          return self.stop(true)
        })
      }
      return self.startPromise
    } else {
      return Promise.resolve()
    }
  }

  // PRIVATE EVENTS: REDIS

  onRedisError (clientId, error) {
    const self = this
    if (error && self.status === FirePitStatus.Started && error.code !== 'ECONNREFUSED') { // otherwise this is handled specifically by self.config.redis.onRetryStrategy
      self.log(`redis error ocurred on client ${clientId}: ${error}`)
      self.stop(true)
    }
  }

  onRedisMessage (messageId, message) {
    const self = this
    const firstnamespaceSeparator = messageId.indexOf(self.config.namespaceSeparator)
    const firePitEvent = messageId.substring(0, firstnamespaceSeparator)
    const typeKey = messageId.substring(firstnamespaceSeparator + 1)
    if (self.subscriptions && self.subscriptions[typeKey] && self.subscriptions[typeKey][firePitEvent]) {
      if (firePitEvent !== FirePitTaskEvent.Process) { // Process needs to have order and be handled when we can
        self.subscriptions[typeKey][firePitEvent](JSON.parse(message))
      }
    }
  }

  // PRIVATE METHODS: REDIS

  getNameForBlockedList (typeKey) {
    const self = this
    return typeKey + self.config.namespaceSeparator + 'blocked' + self.config.namespaceSeparator + self.id
  }

  getNameForReadyList (typeKey) {
    const self = this
    return typeKey + self.config.namespaceSeparator + 'ready'
  }

  getNameForAllSet (typeKey) {
    const self = this
    return typeKey + self.config.namespaceSeparator + 'all'
  }

  getNameForGettingPendingTasks (typeKey) {
    const self = this
    return typeKey + self.config.namespaceSeparator + 'getpendingtasks' + self.config.namespaceSeparator + self.id
  }

  getNameForMetadataKey (dataKey) {
    const self = this
    return dataKey + self.config.namespaceSeparator + 'metadata'
  }

  gettypeAndResourceFromDataKey (dataKey) {
    const self = this
    const dataKeys = dataKey.split(self.config.namespaceSeparator)
    return {
      name: dataKeys[1],
      typeKey: dataKeys.slice(0, 2).join(self.config.namespaceSeparator),
      resource: dataKeys[2],
      resourceKey: dataKeys.slice(0, 3).join(self.config.namespaceSeparator),
      dataKey: dataKey
    }
  }

  publishFirePitTaskEvent (firePitEvent, dataKey, message) {
    const self = this
    const typeAndResource = self.gettypeAndResourceFromDataKey(dataKey)
    const messageToPublish = message || {}
    messageToPublish.id = dataKey
    messageToPublish.name = typeAndResource.name
    messageToPublish.typeKey = typeAndResource.typeKey
    messageToPublish.resource = typeAndResource.resource
    messageToPublish.resourceKey = typeAndResource.resourceKey
    messageToPublish.when = new Date().getTime()
    return self.redisPublisher.publish(firePitEvent + self.config.namespaceSeparator + typeAndResource.typeKey, self.prepareDataForRedis(messageToPublish)).then(_ => {
      return Promise.resolve(messageToPublish)
    }).catch(error => { // eslint-disable-line handle-callback-err
      // if (error) {
      //     self.log(`quiet fail while publishFirePitTaskEvent for ${dataKey}: ${(error.stack) ? error.stack : error.toString()}`)
      // }
      return Promise.resolve(messageToPublish)
    })
  }

  prepareDataForRedis (data) {
    return (typeof data === 'object') ? JSON.stringify(data) : data
  }

  getData (dataKey) {
    const self = this
    return self.redisClient.get(dataKey)
  }

  getMetadata (dataKey) {
    const self = this
    return self.redisClient.get(self.getNameForMetadataKey(dataKey))
  }

  storeData (dataKey, data) {
    const self = this
    return self.redisClient.set(dataKey, self.prepareDataForRedis(data))
  }

  storeMetadata (dataKey, metadata) {
    const self = this
    return self.redisClient.set(self.getNameForMetadataKey(dataKey), self.prepareDataForRedis(metadata))
  }

  removeData (dataKey) {
    const self = this
    return self.redisClient.del(dataKey)
  }

  removeMetadata (dataKey) {
    const self = this
    return self.redisClient.del(self.getNameForMetadataKey(dataKey))
  }

  queueDataForResource (resourceKey, dataKey) {
    const self = this
    return self.redisClient.lpush(resourceKey, dataKey)
  }

  dequeueTaskForResource (resourceKey) {
    const self = this
    return self.redisClient.rpop(resourceKey)
  }

  queueResourceForTypeInReady (typeKey, resourceKey) {
    const self = this
    // Remove from blocked
    return self.redisTransactions.lrem(self.getNameForBlockedList(typeKey), 1, resourceKey).then(_ => {
      return self.redisTransactions.lpush(self.getNameForReadyList(typeKey), resourceKey) // Add to ready
    })
  }

  dequeueResourceForTypeInReady (typeKey) {
    const self = this
    // Get next resource from ready list, and add it to our blocked list
    return self.subscriptionsClients[typeKey].brpoplpush(self.getNameForReadyList(typeKey), self.getNameForBlockedList(typeKey), self.config.dequeueTTL).then(resourceKey => {
      if (resourceKey) {
        return Promise.resolve(resourceKey)
      } else {
        return Promise.reject(new Error(`timed-out while dequeueResourceForTypeInReady for ${typeKey}`))
      }
    })
  }

  queueResourceForType (typeKey, resourceKey) {
    const self = this
    const now = new Date().getTime()
    // Try to add it to the 'all set'
    return self.redisTransactions.hsetnx(self.getNameForAllSet(typeKey), resourceKey, now).then(notExists => {
      // If it didn't exist on the 'all set', then add it to the 'ready queue', because it's a new resource
      // If it exists, then it must be on the ready or blocked queue already
      if (notExists) {
        return self.queueResourceForTypeInReady(typeKey, resourceKey)
      }
      return Promise.resolve()
    })
    // TODO: Resources that are still in the 'blocked list', not blocked, should be re-added to the ready queue by a separate system
  }

  waitUntilAllBlockedTasksFinished (gracefulStopWait, resolveWait) {
    const self = this
    // const gracefulShutdownIteration = (self.config.dequeueTTL * 1000) + 1000 // 1000 to convert to ms, this way we ensure until all have stopped listening for blocking events
    const gracefulShutdownIteration = 100
    if (!resolveWait) {
      return new Promise((resolve, reject) => {
        self.waitUntilAllBlockedTasksFinished(gracefulStopWait, resolve)
      })
    } else {
      if (gracefulStopWait <= 0) {
        resolveWait({ status: 'timed-out' })
      } else {
        // self.log(`graceful shutdown, waiting at least ${gracefulStopWait}ms...`)
        // Wait for first iteration, that way we guarantee all have stopped listening for blocking events
        setTimeout(_ => {
          let promise = Promise.resolve(false) // start as if there are no pending tasks
          Object.keys(self.subscriptions).forEach(typeKey => {
            promise = promise.then(hasPendingTasks => {
              if (!hasPendingTasks && self.redisClient && self.redisClient.status === 'ready') {
                return self.redisClient.exists(self.getNameForBlockedList(typeKey))
              }
              return Promise.resolve(hasPendingTasks)
            })
          })
          return promise.then(hasPendingTasks => {
            if (hasPendingTasks) {
              self.waitUntilAllBlockedTasksFinished(gracefulStopWait - gracefulShutdownIteration, resolveWait)
            } else {
              resolveWait({ status: 'finished' })
            }
          })
        }, gracefulShutdownIteration)
      }
    }
  }

  // PUBLIC METHODS

  start () {
    const self = this
    if (self.config.autostart) {
      return new Promise((resolve, reject) => {
        setTimeout(() => {
          self.doStart().then(resolve).catch(reject)
        }, 10) // Wait 10ms for the auto-start to take place if configured like that
      })
    } else {
      return self.doStart()
    }
  }

  stop (forced) {
    const self = this
    if ([FirePitStatus.Starting, FirePitStatus.Started].indexOf(self.status) >= 0) {
      return self.updateStatus(FirePitStatus.Stopping).then(_ => {
        return self.resetStartTimer()
      }).then(_ => {
        // Wait for pending tasks before shutting down the rest of the clients
        // This takes longer, but we need to do it first
        // Shutting down the blocking clients sometimes leads to unexpected pushes to blocked list
        return self.waitUntilAllBlockedTasksFinished(self.config.gracefulStopWait)
      }).then(_ => {
        const redisClientsToStop = []

        // Stop all blocking clients first, to not continue to listen for new tasks
        Object.keys(self.subscriptionsClients).forEach(key => {
          redisClientsToStop.push(self.subscriptionsClients[key])
        })

        // Stop the rest main 3 clients
        if (self.redisClient) {
          redisClientsToStop.push(self.redisClient)
        }
        if (self.redisSubscriber) {
          redisClientsToStop.push(self.redisSubscriber)
        }
        if (self.redisPublisher) {
          redisClientsToStop.push(self.redisPublisher)
        }
        if (self.redisTransactions) {
          redisClientsToStop.push(self.redisTransactions)
        }

        const clientStopPromises = []
        redisClientsToStop.forEach((client, clientIdx) => {
          clientStopPromises.push(new Promise((resolve, reject) => {
            let didTriggerQuit = false
            let didTriggerShutdownTimer = false
            const forceShutdownTimer = setTimeout(_ => {
              client.removeAllListeners()
              if (!didTriggerQuit) {
                didTriggerShutdownTimer = true
                client.disconnect()
                // self.log(`quit forcefully ${clientIdx + 1}/${redisClientsToStop.length}`)
                resolve()
              }
            }, 10000)
            client.quit(_ => {
              client.removeAllListeners()
              if (!didTriggerShutdownTimer) {
                didTriggerQuit = true
                clearTimeout(forceShutdownTimer)
                // self.log(`quit safely ${clientIdx + 1}/${redisClientsToStop.length}`)
                resolve()
              }
            })
          }))
        })

        return Promise.all(clientStopPromises)
      }).then(_ => {
        return self.updateStatus(FirePitStatus.Stopped)
      }).then(_ => {
        return self.initializeRedisClients()
      })
    }
    return Promise.resolve()
  }

  // TODO: explain options
  // options (optional)
  //

  addTask (type, resource, data, options) {
    const self = this
    if (!type) {
      return Promise.reject(new Error('type is not defined'))
    } else if (typeof type !== 'string') {
      return Promise.reject(new Error('type is not a string'))
    } else if (!type.length) {
      return Promise.reject(new Error('type is an empty string'))
    } else if (type.indexOf(self.config.namespaceSeparator) >= 0) {
      return Promise.reject(new Error('type is invalid, cannot contain ' + self.config.namespaceSeparator))
    } else if (!resource) {
      return Promise.reject(new Error('resource is not defined'))
    } else if (typeof resource !== 'string') {
      return Promise.reject(new Error('resource is not a string'))
    } else if (!resource.length) {
      return Promise.reject(new Error('resource is an empty string'))
    } else if (resource.indexOf(self.config.namespaceSeparator) >= 0) {
      return Promise.reject(new Error('resource is invalid, cannot contain ' + self.config.namespaceSeparator))
    } else if (!data) {
      return Promise.reject(new Error('data is not defined'))
    } else if (typeof data !== 'object') {
      return Promise.reject(new Error('data is not an object'))
    } else {
      const now = new Date().getTime()
      const typeKey = self.config.projectName + self.config.namespaceSeparator + type
      const resourceKey = typeKey + self.config.namespaceSeparator + resource
      const dataKey = resourceKey + self.config.namespaceSeparator + now.toString() + self.config.namespaceSeparator + randomString(8)
      const metadata = {
        added: now
      }
      if (options) {
        if (options.retries) {
          metadata.retries = options.retries // TODO: -1 means infinite retry, should be in a pending to run queue, but failed queue, so we can purge
        }
        if (options.retryInterval) {
          metadata.retryInterval = options.retryInterval // TODO: this should have a minimum
        }
        if (options.retryMaxAge) {
          metadata.retryMaxAge = options.retryMaxAge // TODO: this should have a minimum/maximum
        }
        if (options.delay) {
          var t = new Date()
          t.setMilliseconds(t.getMilliseconds() + options.delay)
          metadata.shouldStartAt = t // TODO
        } else if (options.shouldStartAt) {
          metadata.shouldStartAt = options.shouldStartAt // TODO
        }
      }
      return self.publishFirePitTaskEvent(FirePitTaskEvent.New, dataKey).then(_ => {
        return self.storeData(dataKey, data)
      }).then(_ => {
        return self.storeMetadata(dataKey, metadata)
      }).then(_ => {
        return self.queueDataForResource(resourceKey, dataKey)
      }).then(_ => {
        return self.queueResourceForType(typeKey, resourceKey)
      }).then(_ => {
        const message = {
          id: dataKey,
          type: type,
          resource: resource,
          data: data
        }
        return Promise.resolve(message)
      })
    }
  }

  subscribeToTasks (type, typeMaxTasksAllowed, events) {
    const self = this
    if (!type) {
      return Promise.reject(new Error('type is not defined'))
    } else if (typeof type !== 'string') {
      return Promise.reject(new Error('type is not a string'))
    } else if (!type.length) {
      return Promise.reject(new Error('type is an empty string'))
    } else if (type.indexOf(self.config.namespaceSeparator) >= 0) {
      return Promise.reject(new Error('type is invalid, cannot contain ' + self.config.namespaceSeparator))
    } else if (events[FirePitTaskEvent.Process] && !typeMaxTasksAllowed) {
      return Promise.reject(new Error('typeMaxTasksAllowed should be 1 or more'))
    } else if (events[FirePitTaskEvent.Process] && typeof typeMaxTasksAllowed !== 'number') {
      return Promise.reject(new Error('typeMaxTasksAllowed should be 1 or more'))
    } else if (!events) {
      return Promise.reject(new Error('events is not defined'))
    } else if (typeof events !== 'object') {
      return Promise.reject(new Error('events is not an object'))
    } else if (!Object.keys(events).length) {
      return Promise.reject(new Error('events is empty'))
    } else if (!self.eventsIsValid(events)) {
      return Promise.reject(new Error('events contains invalid events, check FirePitTaskEvent'))
    } else {
      const typeKey = self.config.projectName + self.config.namespaceSeparator + type

      if (!self.subscriptions[typeKey]) {
        self.subscriptions[typeKey] = {}
      }

      let promise = Promise.resolve()

      Object.keys(events).forEach(eventTypeKey => {
        promise = promise.then(_ => {
          if (!self.subscriptions[typeKey][eventTypeKey]) {
            self.subscriptions[typeKey][eventTypeKey] = events[eventTypeKey]
            return Promise.resolve()
          } else {
            return Promise.reject(new Error(`already susbscribed to ${eventTypeKey} in ${typeKey}`))
          }
        })
      })

      promise = promise.then(_ => {
        // Initialize max tasks allowed to 1
        if (!self.subscriptionsMaxTasksAllowed[typeKey]) {
          self.subscriptionsMaxTasksAllowed[typeKey] = 1
        }

        // Save consecutive calls, only to minimum, only t
        if (typeMaxTasksAllowed && typeMaxTasksAllowed < self.subscriptionsMaxTasksAllowed[typeKey]) {
          self.subscriptionsMaxTasksAllowed[typeKey] = typeMaxTasksAllowed
        }
      })

      Object.keys(self.subscriptions[typeKey]).forEach(firePitEvent => {
        promise = promise.then(_ => {
          return self.redisSubscriber.subscribe(firePitEvent + self.config.namespaceSeparator + typeKey)
        })
      })

      return promise.then(_ => {
        var subscriptionPromise = Promise.resolve()
        if (self.subscriptions && self.subscriptions[typeKey] && self.subscriptions[typeKey][FirePitTaskEvent.Process]) {
          subscriptionPromise = subscriptionPromise.then(_ => {
            self.subscriptionsClients[typeKey] = self.createRedisClient()
            self.subscriptionsClients[typeKey].on('error', self.onRedisError.bind(self, `subscription ${typeKey}`))
            return self.subscriptionsClients[typeKey].connect()
          }).then(_ => {
            return self.getPendingTasksForType(typeKey, self.subscriptionsMaxTasksAllowed[typeKey] - 1)
          })
        }
        return subscriptionPromise
      }).then(_ => {
        return Promise.resolve() // to return an empty resolve outside
      })
    }
  }

  subscribeToProcessTasks (type, typeMaxTasksAllowed, callback) {
    const self = this
    const subscriptionEvents = {}
    subscriptionEvents[FirePitTaskEvent.Process] = callback
    return self.subscribeToTasks(type, typeMaxTasksAllowed, subscriptionEvents)
  }

  subscribeToBeNotifiedOfNewTasks (type, callback) {
    const self = this
    const subscriptionEvents = {}
    subscriptionEvents[FirePitTaskEvent.New] = callback
    return self.subscribeToTasks(type, 0, subscriptionEvents)
  }

  subscribeToBeNotifiedOfStartedTasks (type, callback) {
    const self = this
    const subscriptionEvents = {}
    subscriptionEvents[FirePitTaskEvent.Started] = callback
    return self.subscribeToTasks(type, 0, subscriptionEvents)
  }

  subscribeToBeNotifiedOfUpdatedTasks (type, callback) {
    const self = this
    const subscriptionEvents = {}
    subscriptionEvents[FirePitTaskEvent.Updated] = callback
    return self.subscribeToTasks(type, 0, subscriptionEvents)
  }

  subscribeToBeNotifiedOfFinishedTasks (type, callback) {
    const self = this
    const subscriptionEvents = {}
    subscriptionEvents[FirePitTaskEvent.Finished] = callback
    return self.subscribeToTasks(type, 0, subscriptionEvents)
  }

  unsubscribeToTasks (type) {
    const self = this
    if (!type) {
      return Promise.reject(new Error('type is not defined'))
    } else if (typeof type !== 'string') {
      return Promise.reject(new Error('type is not a string'))
    } else if (!type.length) {
      return Promise.reject(new Error('type is an empty string'))
    } else if (type.indexOf(self.config.namespaceSeparator) >= 0) {
      return Promise.reject(new Error('type is invalid, cannot contain ' + self.config.namespaceSeparator))
    } else {
      const typeKey = self.config.projectName + self.config.namespaceSeparator + type
      if (self.subscriptions[typeKey]) {
        let promise = Promise.resolve()
        Object.keys(self.subscriptions[typeKey]).forEach(firePitEvent => {
          promise = promise.then(_ => {
            return self.redisSubscriber.unsubscribe(firePitEvent + self.config.namespaceSeparator + typeKey)
          })
        })
        delete self.subscriptions[typeKey]
        if (self.subscriptionsClients[typeKey]) {
          self.subscriptionsClients[typeKey].removeAllListeners()
          self.subscriptionsClients[typeKey].disconnect()
          delete self.subscriptionsClients[typeKey]
        }
        return Promise.resolve()
      } else {
        return Promise.reject(new Error('not subscribed to ' + type))
      }
    }
  }

  // PUBLIC EVENTS: TASKS

  startTask (dataKey) {
    const self = this
    if (!dataKey) {
      return Promise.reject(new Error('dataKey is not defined'))
    } else if (typeof dataKey !== 'string') {
      return Promise.reject(new Error('dataKey is not a string'))
    } else if (!dataKey.length) {
      return Promise.reject(new Error('dataKey is an empty string'))
    } else {
      return self.publishFirePitTaskEvent(FirePitTaskEvent.Started, dataKey)
    }
  }

  updateTask (dataKey, progress) {
    const self = this
    if (!dataKey) {
      return Promise.reject(new Error('dataKey is not defined'))
    } else if (typeof dataKey !== 'string') {
      return Promise.reject(new Error('dataKey is not a string'))
    } else if (!dataKey.length) {
      return Promise.reject(new Error('dataKey is an empty string'))
    } else if (!progress) {
      return Promise.reject(new Error('invalid progress, needs to be float between bigger than 0 and lower or equal to 1'))
    } else if (parseFloat(progress) > 1) {
      return Promise.reject(new Error('invalid progress, needs to be float between bigger than 0 and lower or equal to 1'))
    } else {
      const message = {
        data: parseFloat(progress)
      }
      return self.publishFirePitTaskEvent(FirePitTaskEvent.Updated, dataKey, message)
    }
  }

  finishTask (dataKey, error) {
    // TODO handle error
    const self = this
    if (!dataKey) {
      return Promise.reject(new Error('dataKey is not defined'))
    } else if (typeof dataKey !== 'string') {
      return Promise.reject(new Error('dataKey is not a string'))
    } else if (!dataKey.length) {
      return Promise.reject(new Error('dataKey is an empty string'))
    } else {
      const typeAndResource = self.gettypeAndResourceFromDataKey(dataKey)
      return self.publishFirePitTaskEvent(FirePitTaskEvent.Finished, dataKey).then(_ => {
        return self.removeData(dataKey)
      }).then(_ => {
        return self.removeMetadata(dataKey)
      }).then(_ => {
        return self.queueResourceForTypeInReady(typeAndResource.typeKey, typeAndResource.resourceKey)
      }).then(_ => {
        return self.getPendingTasksForType(typeAndResource.typeKey)
      }).catch(error => {
        if (error) {
          self.log(`failed while finishing task ${dataKey}: ${(error.stack) ? error.stack : error.toString()}`)
        }
        return self.stop(true)
      })
    }
  }
}

module.exports = {
  FirePit: FirePit,
  FirePitStatus: FirePitStatus,
  FirePitEvent: FirePitEvent,
  FirePitTaskEvent: FirePitTaskEvent
}
