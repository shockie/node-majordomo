var messages = require('./messages'),
	request = require('request'),
	zmq = require('zmq');


function Worker(endpoint, service){
	this.connected = false;
	this.socket = zmq.socket('dealer');
	this.socket.connect(endpoint);
	this.socket.on('message', this.onMessage.bind(this));
	this.service = service;

	this.ready();
}

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
	this.socket.disconnect();
	this.connected = false;
};

Worker.prototype.onRequest = function(message){
	console.log('received payload: %s', Buffer.concat(message.data).toString());
	this.socket.send(new messages.worker.FinalMessage(message.service, JSON.stringify({
		ping: 'pong'
	})).toFrames());
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
	this.socket.send(new messages.worker.DisconnectMessage().toFrames());
	//notify the broker to remove this worker from the pool
};

if(require.main){
	var worker = new Worker('tcp://127.0.0.1:5555', 'blocks:http-request');
	process.on('exit', function(){
		console.log('Worker: exit');
	});

	process.on('SIGINT', function(){
		worker.disconnect();
		console.log('SIGINT');
		process.exit();
	});
}