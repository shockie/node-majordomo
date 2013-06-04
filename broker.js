var util = require('util'),
	events = require('events'),
	messages = require('./messages'),
	redis = require('redis'),
	zmq = require('zmq'),
	router = zmq.socket('router'),
	clientRouter = zmq.socket('router'),
	workerRouter = zmq.socket('router');

var Broker = exports.Broker = function Broker(endpoint){

	this.services = {};
	this.queue = [];
	this.workers = 0;

	this.socket = zmq.socket('router');
	this.socket.bindSync(endpoint);
	this.socket.on('message', this.onMessage.bind(this));

	setInterval(this.dispatch.bind(this), 0);
};

Object.defineProperty(Broker.prototype, 'available', {
	get: function(){
		var keys = Object.keys(this.services),
			available = 0;
		if(!keys.length){
			return false;
		}

		keys.forEach(function(key){
			available = available + this.services[key].waiting.length;
		}, this);

		return available;
	}
});

Object.defineProperty(Broker.prototype, 'ready', {
	get: function(){
		return this.available > 0;
	}
});


Broker.prototype.onWorkerReady = function(message, envelope){
	console.log('Worker registers itself');
	if(!this.services.hasOwnProperty(message.service.toString())){
		console.log("broker doesn't yet understand %s", message.service.toString());
		this.services[message.service.toString()] = {
			waiting: [],
			workers: 0
		};
	}

	this.services[message.service.toString()].workers++;
	this.services[message.service.toString()].waiting.push(message.envelope);
};

Broker.prototype.onClientRequest = function(message){
	// add reqeust to queue
	// on every tick, check workers queue if any available
	// if available dispatch, other wise wait
	// if request doesn't get a free worker within some period, send timeout

	if(this.services.hasOwnProperty(message.service.toString())){
		this.queue.push(message);
		//lookup a valid worker
	}else{
		//TODO: send MDPC_NAK
	}
};

Broker.prototype.dispatch = function(){
	if(this.queue.length && this.ready){
		var message = this.queue.shift(),
			worker = this.services[message.service.toString()].waiting.pop();

		// console.log('go to worker: %s, total idle workers: %s', worker.toString(), this.available);
		this.socket.send(new messages.worker.RequestMessage(message.envelope, message.data, worker).toFrames());
		this.services[message.service.toString()].waiting.unshift(worker);
	}
};

Broker.prototype.onWorkerFinal = function(message){
	//worker lookup by envelope
	//
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
		if(this.services[service].waiting.some(function(worker){
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
	this.services[service].waiting.forEach(function(worker, index){
		if(worker.toString() == sender.toString()){
			knownIndex = index;
		}
	});
	return knownIndex;
};

Broker.prototype.onWorkerDisconnect = function(message){
	var service = this.findServiceBySender(message.envelope),
		index = this.findIndexBySenderService(message.envelope, service);

	this.services[service].waiting.splice(index, 1);
	this.services[service].workers--;


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
	var keys = Object.keys(this.services);
	if(!keys.length){
		return;
	}
	//loop over all known workers and send a disconnect to them
	keys.forEach(function(service){
		if(!this.services[service].workers){
			return;
		}

		this.services[service].waiting.forEach(function(worker){
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