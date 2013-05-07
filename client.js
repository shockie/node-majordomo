var messages = require('./messages'),
	zmq = require('zmq'),
	client = zmq.socket('dealer');

client.connect('tcp://127.0.0.1:5555');

var buffer = [];

client.on('partial', function(type, data){
	buffer.push(data);
});

client.on('finish', function(type, data){
	buffer.push(data);
	//we're done
	//
	console.log(Buffer.concat(buffer).toString());
	buffer = [];
});

client.on('message', function(protocol, type, service){
	console.log('received message', Buffer.concat(Array.prototype.slice.call(arguments)).toString());

	var data = Buffer.concat(Array.prototype.slice.call(arguments, 3));
	console.log(arguments[1].toString(), type.toString());
	if(type == messages.client.PARTIAL){
		client.emit('partial', service, data);
	} else if(type == messages.client.FINAL){
		client.emit('finish', service, data);
	}
});

for(var i = 0; i < 5; i++){
	client.send([
		'MDPC02',
		messages.client.REQUEST,
		'blocks:http-request',
		JSON.stringify({url: 'http://www.example.org/'})
	]);
}