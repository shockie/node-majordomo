var util = require('util'),
	events = require('events'),
	messages = require('./messages'),
	redis = require('redis'),
	zmq = require('zmq'),
	router = zmq.socket('router'),
	clientRouter = zmq.socket('router'),
	workerRouter = zmq.socket('router');

var Broker = function(endpoint){
	events.EventEmitter.call(this);
	this.services = {};
	this.waiting = [];

	this.socket = zmq.socket('router');
	this.socket.bindSync(endpoint);
	this.socket.on('message', this.onMessage.bind(this));
};

util.inherits(Broker, events.EventEmitter);


Broker.prototype.onWorkerReady = function(message, envelope){
	console.log('Worker registers itself');
	if(!this.services.hasOwnProperty(message.service.toString())){
		console.log("broker doesn't yet understand %s", message.service.toString());
		this.services[message.service.toString()] = [];
	}

	this.services[message.service.toString()].push(message.envelope);
};

Broker.prototype.onClientRequest = function(message){
	console.log('client message');
	if(this.services.hasOwnProperty(message.service.toString()) && this.services[message.service.toString()].length){
		//lookup a valid worker
		var worker = this.services[message.service.toString()].pop();
		console.log('go to worker: %s, total idle workers: %s', worker.toString(), this.services[message.service.toString()].length);
		this.socket.send(new messages.worker.RequestMessage(message.envelope, message.data, worker).toFrames());
		this.services[message.service.toString()].unshift(worker);
	}
};

Broker.prototype.onWorkerFinal = function(message){
	//worker lookup by envelope
	var service = this.findServiceBySender(message.envelope);
	this.socket.send(new messages.client.FinalMessage(service, message.data, message.service).toFrames());
};

Broker.prototype.onWorkerPartial = function(message){
	//worker lookup by envelope
	var service = this.findServiceBySender(message.envelope);
	this.socket.send(new messages.client.PartialMessage(service, message.data, message.service).toFrames());
};

Broker.prototype.findServiceBySender = function(sender){
	var knownService;

	Object.keys(this.services).forEach(function(service){
		if(this.services[service].some(function(worker){
			return sender.toString() == worker.toString();
		})){
			knownService = service;
		}
		return;
	}, this);

	return knownService;
};

Broker.prototype.onWorkerDisconnect = function(message){
	var service = this.findServiceBySender(message.envelope);

	//indexOf quirks
	// var index = this.services[services];

	//splice the service array;
	//done
};

Broker.prototype.onMessage = function(envelope, protocol, type) {
	var message = messages.fromFrames(arguments, true);

	if(message instanceof messages.client.Message){
		if(message instanceof messages.client.RequestMessage){
			this.onClientRequest(message);
		}
	}else if(message instanceof messages.worker.Message){
		//
		if(message instanceof messages.worker.ReadyMessage){
			this.onWorkerReady(message, envelope);
		} else if(message instanceof messages.worker.PartialMessage){
			this.onWorkerPartial(message);
		} else if(message instanceof messages.worker.FinalMessage){
			this.onWorkerFinal(message);
		} else if(message instanceof messages.worker.DisconnectMessage){
			this.onWorkerDisconnect(message);
		}
	}
};

if(require.main){
	var broker = new Broker('tcp://127.0.0.1:5555');
}