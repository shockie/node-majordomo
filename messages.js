var util = require('util');

var protocols = {
	client: 'MDPC02',
	worker: 'MDPW02'
};

var types = {
	client: {
		REQUEST: 0x01,
		PARTIAL: 0x02,
		FINAL: 0x03
	},
	worker: {
		READY: 0x01,
		REQUEST: 0x02,
		PARTIAL: 0x03,
		FINAL: 0x04,
		HEARTBEAT: 0x05,
		DISCONNECT: 0x06
	}
};

function Message(protocol, type){
	return [protocol, type];
}

function ClientMessage(type, service, data){
	return Message.call(this, protocols.client, types.client[type.toUpperCase()]).concat([service, data]);
}

util.inherits(ClientMessage, Message);

function WorkerMessage(type, service, data){
	var block = [];
	if(service){
		block.push(service);
		if(data){
			//this is a reply to the client
			block.push('');
			if(Array.isArray(data)){
				block = block.concat(data);
			}else{
				block.push(data);
			}
		}
	}
	return Message.call(this, protocols.worker, types.worker[type.toUpperCase()]).concat(block);
}

util.inherits(WorkerMessage, Message);


module.exports = {
	client: {
		request: function(service, data){
			return new ClientMessage('request', service, data);
		},
		partial: function(service, data){
			return new ClientMessage('partial', service, data);
		},
		final: function(service, data){
			return new ClientMessage('final', service, data);
		}
	},
	worker: {
		ready: function(service){
			return new WorkerMessage('ready', service);
		},
		request: function(client, data){
			return new WorkerMessage('request', client, data);
		},
		partial: function(client, data){
			return new WorkerMessage('partial', client, data);
		},
		final: function(client, data){
			return new WorkerMessage('final', client, data);
		},
		heartbeat: function(){
			return new WorkerMessage('heartbeat');
		},
		disconnect: function(){
			return new WorkerMessage('disconnect');
		}
	}
};