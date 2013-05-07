var messages = require('./messages'),
	request = require('request'),
	zmq = require('zmq');


function Worker(endpoint, service){
	this.socket = zmq.socket('dealer');
	this.socket.connect(endpoint);
	this.on('message', this.onMessage.bind(this));
	this.service = service;

	this.register();
}

Worker.prototype.onMessage = function(){
	//transform incoming zmq-frames into a message
};

Worker.prototype.register = function(){
	this.socket.send(messages.worker.ready(this.service));
};

worker.connect('tcp://127.0.0.1:5555');
worker.identity = 'worker' + process.pid;

worker.on('message', function(protocol, type, client, delimeter, data){
	//do something;
	data = JSON.parse(data.toString());
	request(data, function(err, response, body){
		console.log(body);
		worker.send([
			'MDPW02',
			messages.worker.FINAL,
			client,
			'',
			JSON.stringify({
				body: body,
				status: response.status
			})
		]);
	});
});

worker.send(messages.worker.ready('blocks:http-request'));