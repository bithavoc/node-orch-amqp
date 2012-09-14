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
  assert.ok(amqpOptions, "amqpOptions are required in order to create a AmqpTasksSource");
  Object.defineProperty(this, 'amqpOptions', {
    value: amqpOptions,
    enumerable: false,
    configurable: false
  });
  TasksSource.apply(this, arguments);
  this.processing = false;
  this.connection = null;
  this._amqpQueue = null;
  this._amqpExchange = null;
  this.queueName = 'orch';
  this.exchangeName = 'orch';
  this.routingKey = 'tasks';
}
util.inherits(AmqpTasksSource, TasksSource);

AmqpTasksSource.prototype.onNext = function onNext() {
  if(this.processing) {
    // send ACK
    assert.ok(this._amqpQueue, '_amqpQueue should not be empty');
    this._amqpQueue.shift();
    return true;
  }
  return false;
};

AmqpTasksSource.prototype._notifyTask = function _notifyTask(task) {
  this.processing = true;
  this.emit('task', task);
};

AmqpTasksSource.prototype.onEnqueue = function onEnqueue(task, cb) {
  this._amqpExchange.publish(this.routingKey, JSON.stringify(task),{
    deliveryMode: 2 // persistent
  });
  return cb();
};

AmqpTasksSource.prototype.onConnect = function onConnect(cb) {
  var self = this;
  this.connection = amqp.createConnection(this.amqpOptions);
  this.connection.once('ready', function amqpConnectionReady() {
    var exchange = self._amqpExchange = self.connection.exchange(self.exchangeName, {
      type: 'direct',
      durable: true,
      autoDelete: false
    }, function amqpExchangeReady() {
      console.log("-> Exchange Ready");
      self.connection.queue(self.queueName, {
        autoDelete: false,
        durable: true
      }, function amqpQueueReady(q) {
        self._amqpQueue = q;
        console.log("-> Connected to Queue");
        q.bind(exchange, self.routingKey);
        if(self.subscribe) {
          q.subscribe({ack: true}, function amqpMessageArrived(message, headers, mode) {
            var taskObj = JSON.parse(message.data.toString());
            return self._notifyTask(taskObj);
          }); // messageArrive
        }
        return cb();
      }); // queueReady
    }); // exchangeReady
  }); // connectionReady 
}; // onConnect

module.exports = AmqpTasksSource;
