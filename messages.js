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

function Message(protocol, type, service, data, envelope){
	this.protocol = protocol;
	this.type = type;
	this.service = service;
	this.data = data;
	this.envelope = envelope;
	// return [protocol, type];
}

Message.prototype.toFrames = function(){
	var frames = [];

	if(this.envelope){
		frames.push(this.envelope);
	}

	frames.push(this.protocol);
	frames.push(this.type);

	if(this.service){
		frames.push(this.service);
	}

	if(this.data){
		frames.push('');

		if(Array.isArray(this.data)){
			frames = frames.concat(this.data);
		}else{
			frames.push(this.data);
		}
	}

	return frames;
};

function fromFrames(frames, hasEnvelope){
	frames = Array.prototype.slice.call(frames);
	var protocol = frames[0],
		type = frames[1],
		service = frames[2],
		data = frames.slice(4),
		envelope;

	if(hasEnvelope){
		protocol = frames[1];
		type = frames[2];
		service = frames[3];
		data = frames.slice(5);
		envelope = frames[0];
	}

	if(envelope){
		console.log('envelope: %s', envelope.toString());
	}
	console.log('protocol: %s', protocol.toString());
	console.log('type: %s', type.toString());
	if(service){
		console.log('service: %s', service.toString());
	}
	if(data){
		console.log('data: %s (%s frames)', Buffer.concat(data).toString(), data.length);
	}

	//check if broker receives the message, offset + 1
	if(protocol == protocols.client){
		if(type == types.client.REQUEST){
			return new ClientRequestMessage(service, data, envelope);
		}else if(type == types.client.PARTIAL){
			return new ClientPartialMessage(service, data);
		}else if(type == types.client.FINAL){
			return new ClientFinalMessage(service, data, envelope);
		}
	} else if(protocol == protocols.worker){
		//time to go into workermode
		if(type == types.worker.READY){
			return new WorkerReadyMessage(service, envelope);
		} else if(type == types.worker.REQUEST){
			return new WorkerRequestMessage(service, data);
		} else if(type == types.worker.PARTIAL){
			return new WorkerPartialMessage(service, data, envelope);
		} else if(type == types.worker.FINAL){
			return new WorkerFinalMessage(service, data, envelope);
		} else if(type == types.worker.HEARTBEAT){
			return new WorkerHeartbeatMessage(envelope);
		} else if(type == types.worker.DISCONNECT){
			return new WorkerDisconnectMessage(envelope);
		}
	}
	//cast into function;
	return;
}


function ClientMessage(type, service, data, envelope){
	Message.call(this, protocols.client, types.client[type.toUpperCase()], service, data, envelope);
}

util.inherits(ClientMessage, Message);


function WorkerMessage(type, service, data, envelope){
	Message.call(this, protocols.worker, types.worker[type.toUpperCase()], service, data, envelope);
}

util.inherits(WorkerMessage, Message);


/**
 * Client Messages
 */

function ClientRequestMessage(service, data, envelope){
	ClientMessage.call(this, 'request', service, data, envelope);
}

util.inherits(ClientRequestMessage, ClientMessage);


function ClientPartialMessage(service, data, envelope){
	ClientMessage.call(this, 'partial', service, data, envelope);
}

util.inherits(ClientPartialMessage, ClientMessage);


function ClientFinalMessage(service, data, envelope){
	ClientMessage.call(this, 'final', service, data, envelope);
}

util.inherits(ClientFinalMessage, ClientMessage);


/**
 * Worker Messages
 */

function WorkerReadyMessage(service, envelope){
	WorkerMessage.call(this, 'ready', service, null, envelope);
}

util.inherits(WorkerReadyMessage, WorkerMessage);


function WorkerRequestMessage(client, data, envelope){
	WorkerMessage.call(this, 'request', client, data, envelope);
}

util.inherits(WorkerRequestMessage, WorkerMessage);


function WorkerPartialMessage(client, data, envelope){
	WorkerMessage.call(this, 'partial', client, data, envelope);
}

util.inherits(WorkerPartialMessage, WorkerMessage);


function WorkerFinalMessage(client, data, envelope){
	WorkerMessage.call(this, 'final', client, data, envelope);
}

util.inherits(WorkerFinalMessage, WorkerMessage);


function WorkerHeartbeatMessage(envelope){
	WorkerMessage.call(this, 'heartbeat', null, null, envelope);
}

util.inherits(WorkerHeartbeatMessage, WorkerMessage);


function WorkerDisconnectMessage(envelope){
	WorkerMessage.call(this, 'disconnect', null, null, envelope);
}

util.inherits(WorkerDisconnectMessage, WorkerMessage);


module.exports = {
	fromFrames: fromFrames,
	client: {
		Message: ClientMessage,
		RequestMessage: ClientRequestMessage,
		PartialMessage: ClientPartialMessage,
		FinalMessage: ClientFinalMessage
	},
	worker: {
		Message: WorkerMessage,
		ReadyMessage: WorkerReadyMessage,
		RequestMessage: WorkerRequestMessage,
		PartialMessage: WorkerPartialMessage,
		FinalMessage: WorkerFinalMessage,
		HeartbeatMessage: WorkerHeartbeatMessage,
		DisconnectMessage: WorkerDisconnectMessage
	}
};