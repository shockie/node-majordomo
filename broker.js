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

Broker.prototype.findIndexBySenderService = function(sender, service){
	var knownIndex = -1;
	this.services[service].forEach(function(worker, index){
		console.log('LOOPING SERVICE');
		console.log(worker.toString() == sender.toString());
		if(worker.toString() == sender.toString()){
			knownIndex = index;
		}
	});
	return knownIndex;
};

Broker.prototype.onWorkerDisconnect = function(message){
	var service = this.findServiceBySender(message.envelope),
		index = this.findIndexBySenderService(message.envelope, service);

	this.services[service].splice(index);


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

Broker.prototype.disconnectWorker = function(envelope){
	this.socket.send(new messages.worker.DisconnectMessage(envelope).toFrames());
};

Broker.prototype.disconnect = function(){
	console.log('disconnecting workers');
	if(!Object.keys(this.services).length){
		return;
	}
	//loop over all known workers and send a disconnect to them
	Object.keys(this.services).forEach(function(service){
		if(!this.services[service].length){
			return;
		}

		this.services[service].forEach(function(worker){
			this.disconnectWorker(worker);
		}, this);
	}, this);
};

if(require.main){
	var broker = new Broker('tcp://127.0.0.1:5555');

	process.on('SIGINT', function(){
		broker.disconnect();
		process.exit();
	});
}