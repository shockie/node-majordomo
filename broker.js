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

	this.socket = router;
	this.services = {};
	this.waiting = [];
	router.bindSync(endpoint);

	router.on('message', this.onMessage.bind(this));
};

util.inherits(Broker, events.EventEmitter);


Broker.prototype.onClientMessage = function(envelope, type, service, data){
	console.log('client message');
	if(this.services.hasOwnProperty(service.toString()) != -1){
		//lookup a valid worker
		var worker = this.services[service.toString()].pop();
		console.log('go to worker: %s, total workers: %s', worker.toString(), this.services[service.toString()].length);

		this.socket.send([
			worker,
			'MDPW02',
			messages.worker.request,
			envelope,
			'',
			Buffer.concat(data)
		]);

		this.services[service.toString()].unshift(worker);
	}
};

Broker.prototype.onWorkerMessage = function(envelope, type, service, data){
	if(type == messages.worker.READY){
		this.onWorkerReady.call(this, envelope, service);
	}else{
		console.log('no match');
	}
};

Broker.prototype.onClientToWorkerMessage = function(envelope, type, client, data){
	//FIXME: a reference to the service name
	this.socket.send([
		client,
		'MDPC02',
		messages.client.FINAL,
		'blocks:http-request',
		data
	]);
};

Broker.prototype.onWorkerReady = function(envelope, service){
	console.log('Worker registers itself');
	if(!this.services.hasOwnProperty(service.toString())){
		console.log("broker doesn't yet understand %s", service.toString());
		this.services[service.toString()] = [];
	}
	// if(this.services[service.toString()].indexOf(envelope) == -1){
		this.services[service.toString()].push(envelope);
	// }
};


Broker.prototype.onMessage = function(envelope, protocol, type) {
	var args = Array.prototype.slice.call(arguments);
	data = Array.prototype.slice.call(arguments, 4);
	if(protocol == 'MDPW02'){
		if(args.length > 5){
			//this mean we have a zero-byte delimeted(partial,final) message
			this.onClientToWorkerMessage.call(this, envelope, type, args[3], args.slice(5));
		}else{
			this.onWorkerMessage.call(this, envelope, type, args[3]);
		}
	}else if(protocol == 'MDPC02'){
		// this is a message from a client
		console.log(Buffer.concat(args).toString(), args[3].toString());
		this.onClientMessage.call(this, envelope, type, args[3], data);
	}
};

if(require.main){
	var broker = new Broker('tcp://127.0.0.1:5555');
}

// var client = redis.createClient();


// clientRouter.bindSync('tcp://127.0.0.1:5555');
// workerRouter.bindSync('tcp://127.0.0.1:6666');

// workerRouter.messages = {
// 	ready: 0x01,
// 	request: 0x02,
// 	partial: 0x03,
// 	final: 0x04,
// 	heartbeat: 0x05,
// 	disconnect: 0x06
// };

// //we need a lookup table


// clientRouter.on('message', function(envelope, protocol, type, service, data){
// 	client.lrange(service, 0, -1, function(err, workers){
// 		var message =[
// 			new Buffer(workers[0]),
// 			'MDPW02',
// 			workerRouter.messages.request,
// 			envelope,
// 			''];

// 		console.log(message.concat(Array.prototype.slice.call(arguments, 4)));
// 		workerRouter.send(message.concat(Array.prototype.slice.call(arguments, 4)));
// 	// 	console.log(workers);

// 	// 	clientRouter.send([
// 	// 		envelope,
// 	// 		'MDPC02',
// 	// 		0x02,
// 	// 		service,
// 	// 		JSON.stringify({headers: [], status:200, body: ''})
// 	// 	]);

// 	// 	clientRouter.send([
// 	// 		envelope,
// 	// 		'MDPC02',
// 	// 		0x03,
// 	// 		service,
// 	// 		''
// 	// 	]);
// 	});
// });

// workerRouter.on('ready', function(service, envelope){
// 	client.lrange(service, 0, -1, function(err, workers){
// 		if(workers.indexOf(envelope.toString()) == -1){
// 			client.lpush(service, envelope);
// 		}
// 	});
// });


// workerRouter.on('message', function(envelope, protocol, type, service, data){
// 	var messages = Buffer.concat(Array.prototype.slice.call(arguments, 4));

// 	if(type == workerRouter.messages.ready){
// 		workerRouter.emit('ready', service, envelope);
// 	}
// 	console.log(arguments);
// });