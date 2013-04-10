var amqp = require('amqp');
var TasksSource = require('orch').TasksSource;
var util = require('util');
var assert = require('assert');

//
// Default Implementation of TasksSource for AMQP(usually RabbitMQ).
// Events:
//  - ack: ACK signal was sent to Rabbit.mq through the call of .shift();
//
function AmqpTasksSource(amqpOptions) {
  TasksSource.apply(this, arguments);
  assert.ok(amqpOptions, "amqpOptions are required in order to create a AmqpTasksSource");
  Object.defineProperty(this, 'amqpOptions', {
    value: amqpOptions,
    enumerable: false,
    configurable: false
  });
  this.connection = null;
  this._amqpExchange = null;
  this.exchangeName = 'orch';
  this._queues = {};
}
util.inherits(AmqpTasksSource, TasksSource);

AmqpTasksSource.prototype.onIssueQueue = function onIssueQueue(action, cb, autoDelete) {
  var self = this;
  var queueName = action;
  var queue = this._queues[queueName];
  autoDelete = Boolean(autoDelete);
  if(queue) {
    return cb(null, queue);
  }
  var wrapper = new AmqpQueue(self);
  self._queues[queueName] = wrapper;
  
  this.connection.queue(action, {
    autoDelete: autoDelete,
    durable: !autoDelete
  }, function amqpQueueReady(queue) {
    wrapper._queue = queue;
    // The issuing is finished only when the bind is complete, otherwise the bind might not ready
    // and we could potentially loose messages.
    queue.once('queueBindOk', function bindOk() {
      // Once bound, the issuing is finished.
      return cb(null, wrapper);
    });
    queue.bind(self._amqpExchange, action);
  }); // queueReady
}

AmqpTasksSource.prototype.onConnect = function onConnect(cb) {
  var self = this;
  this.connection = amqp.createConnection(this.amqpOptions);
  this.connection.once('ready', function amqpConnectionReady() {
    var exchange = self._amqpExchange = self.connection.exchange(self.exchangeName, {
      type: 'direct',
      durable: true,
      autoDelete: false
    }, function amqpExchangeReady() {
      return cb();
    }); // exchangeReady
  }); // connectionReady 
}; // onConnect


//
// AMQP Queue Wrapper
//
function AmqpQueue(source) {
  assert.ok(source);
  TasksSource.Queue.apply(this, []);
  this.source = source;
  this._amqpProcessing = false;
}
util.inherits(AmqpQueue, TasksSource.Queue);

AmqpQueue.prototype.onNext = function onNext() {
  if(this._isProcessing) {
    // send ACK
    this._isProcessing = false;
    this._queue.shift();
  } else {
    // ack ignored
  }
};

AmqpQueue.prototype._notifyTask = function _notifyTask(task) {
  this._isProcessing = true;
  this.emit('task', task);
};

AmqpQueue.prototype.onListen = function onListen(cb) {
  var self = this;
  this._queue.subscribe({ack: true}, function queueMessageArrived(message, headers, mode) {
    var taskObj = JSON.parse(message.data.toString());
    self._notifyTask(taskObj);
  }); // messageArrive
  return cb(null);
}

AmqpQueue.prototype.onEnqueue = function onQueue(action, task, cb) {
  this.source._amqpExchange.publish(action, JSON.stringify(task), {
    deliveryMode: 2 // persistent
  });
  return cb();
}

module.exports = AmqpTasksSource;
