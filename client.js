var messages = require('./messages'),
	zmq = require('zmq'),
	client = zmq.socket('dealer');

var Client = exports.Client = function Client(options, callback){
	this.socket = zmq.socket('dealer');
	this.socket.connect(options.broker);
	this.socket.on('message', this.onMessage.bind(this));
	this.callback = callback;
	this.done = false;
	this.buffer = [];
	this.request(options.service, options.data, options.timeout || 250);
};

Client.prototype.onMessage = function(){
	var message = messages.fromFrames(arguments);
	if(message instanceof messages.client.PartialMessage){
		this.onPartial(messsage);
	}else if(message instanceof messages.client.FinalMessage){
		this.onFinal(message);
	}
};

Client.prototype.onPartial = function(message){
	this.buffer.push(message);
};


Client.prototype.onFinal = function(message){
	if(this.callback){
		clearTimeout(this.timeout);
		this.buffer.push(message);
		this.done = true;
		this.callback(null, JSON.parse(Buffer.concat(Buffer.concat(this.buffer).data)));
	}
};

Client.prototype.request = function(service, data, timeout){
	this.socket.send(new messages.client.RequestMessage(service, JSON.stringify(data)).toFrames());
	this.timeout = setTimeout(function(){
		if(!this.done){
			this.callback(new Error('timeout'));
		}
		delete this.callback;
	}.bind(this), timeout);
};

if(require.main){
	new Client({
		broker:'tcp://127.0.0.1:5555',
		service: 'blocks:http-request',
		data: {
			url: 'http://www.example.org/'
		},
		timeout: 2000
	}, function(err,data){
		console.log('errored', err instanceof Error);
		console.log(data);
	});
}