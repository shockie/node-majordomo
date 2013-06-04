var messages = require('./messages'),
	request = require('request'),
	zmq = require('zmq');



var Worker = exports.Worker = function Worker(options, action){
	this.connected = false;
	this.socket = zmq.socket('dealer');
	this.socket.connect(options.broker);
	this.socket.on('message', this.onMessage.bind(this));
	this.service = options.service;
	this.action = action;

	this.ready();
};

Worker.prototype.onMessage = function(){
	//transform incoming zmq-frames into a message
	var message = messages.fromFrames(arguments);

	if(message instanceof messages.worker.RequestMessage){
		this.onRequest(message);
	} else if(message instanceof messages.worker.DisconnectMessage){
		this.onDisconnect(message);
	} else{
		console.log('invalid request');
	}
};

Worker.prototype.onDisconnect = function(message){
	console.log('received disconnect request');
	this.socket.disconnect(this.socket.last_endpoint);
	this.connected = false;
	process.exit();
};

Worker.prototype.onRequest = function(message){
	var data = JSON.parse(Buffer.concat(message.data).toString());

	this.action(data, function(err, returnValue){
		this.socket.send(new messages.worker.FinalMessage(message.service, JSON.stringify(returnValue)).toFrames());
	}.bind(this));
};

Worker.prototype.ready = function(){
	if(!this.connected){
		this.socket.send(new messages.worker.ReadyMessage(this.service).toFrames());
		this.connected = true;
	}else{
		console.log('worker is already connected to the broker');
	}
};

Worker.prototype.disconnect = function(){
	if(this.connected){
		this.socket.send(new messages.worker.DisconnectMessage().toFrames());
		this.socket.disconnect(this.socket.last_endpoint);
		this.connected = false;
	}
	//notify the broker to remove this worker from the pool
};

if(require.main){
	var worker = new Worker({
		broker: 'tcp://127.0.0.1:5555',
		service: 'blocks:http-request'
	}, function(data, cb){
		request(data, function(err, response, body){
			cb(err, {
				status: response.statusCode,
				body: body
			});
		});
	});
	process.on('exit', function(){
		console.log('Worker: exit');
	});

	process.on('SIGINT', function(){
		worker.disconnect();
		console.log('SIGINT');
		process.exit();
	});
}