var messages = require('./messages'),
	zmq = require('zmq'),
	client = zmq.socket('dealer');

var requests = 0,
	max = 30;

function Client(broker){
	this.socket = zmq.socket('dealer');
	this.socket.connect(broker);
	this.socket.on('message', this.onMessage.bind(this));
}

Client.prototype.onMessage = function(){
	var message = messages.fromFrames(arguments);
	if(message instanceof messages.client.PartialMessage){
		this.onPartial(messsage);
	}else if(message instanceof messages.client.FinalMessage){
		this.onFinal(message);
	}
};

Client.prototype.onPartial = function(message){
	console.log('partial');
};
Client.prototype.onFinal = function(message){
	requests++;
	if(requests == max){
		console.timeEnd('10-requests');
		process.exit();
	}
};

Client.prototype.request = function(service, data){
	this.socket.send(new messages.client.RequestMessage(service, data).toFrames());
};

if(require.main){
	var client = new Client('tcp://127.0.0.1:5555');
	console.time('10-requests');
	for(var i=0; i< max; i++){
		client.request('blocks:http-request', JSON.stringify({
			url: 'http://www.example.org/'
		}));
	}
}