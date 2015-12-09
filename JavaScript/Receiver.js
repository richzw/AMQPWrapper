'use strict';

/**
 * Dependencies
 */
var amqp = require('amqplib'),
	when = require('when'),
	_ = require('lodash');

/**
 * Receiver Class, message receiver from amqp
 * @param {Object} config The amqp receiver configuration
 */
var Receiver = function( config ) {
	/**
	 * amqp address
	 * @type {!string}
	 * @private
	 */
	var defaultUrl = {
		userName: 'guest',
		password: 'guest',
		host: 'localhost',
		port: 5672,
		vHost: '',
	};
	config.url = _.extend(defaultUrl, config.url);

	this.amqpUrl_ = 'amqp://' + config.url.userName + ':'
		+ config.url.password + '@'
		+ config.url.host + ':'
		+ config.url.port
		+ config.url.vHost;

	/**
	 * retry delay time
	 * @type {number}
	 * @private
	 */
	this.retryDelay_ = config.retryDelay || 10000;

	var defaultConnectOpt = {
		'noDelay': true,
		'channelMax': 0,
		'frameMax': 0,
		'heartbeat': 0, // per second
	};
	this.connectOpt_ = _.extend( defaultConnectOpt, config.connectOpt );

	var defaultQueueOpt = {
		'exclusive': false,
		'durable': true,
		'autoDelete': false,
	};

	this.queueOpt_ = _.extend( defaultQueueOpt, config.queueOpt );

	var defaultExchangeOpt = {
		'durable': true,
		'internal': false,
		'autoDelete': false,
	};
	this.exchangeOpt_ = _.extend( defaultExchangeOpt, config.exchangeOpt );

	var defaultConsumeOpt = {
		'consumerTag': 'BONUSUI',
		'noLocal': false,
		'noAck': true,
		//'exclusive': true,
	};
	this.consumerOpt_ = _.extend( defaultConsumeOpt, config.consumerOpt );

	/**
	 * amqp connection
	 * @type {Object}
	 * @private
	 */
	this.con_ = null;
	/**
	 * amqp channel
	 * @type {Object}
	 * @private
	 */
	this.ch_ = null;
	
	/**
	 * prefetch count, prevent bunch of messages flooding in to memory issue.
	 * @type {Number}
	 * @private
	 */
	this.prefetchCount_ = config.receiverConfig.prefetchCound || 30;
	
	/**
	 * exchange name
	 * @type {string}
	 * @private
	 */
	this.ex_ = config.receiverConfig.exchangeName || 'UI';
	/**
	 * queue name
	 * @type {string}
	 * @private
	 */
	this.queueName_ = config.receiverConfig.queueName || 'UI';
	/**
	 * message receiver callback
	 * @type {Function}
	 * @private
	 */
	this.cb_ = config.receiverConfig.callback;

	this.routingKey_ = config.receiverConfig.routingKey || 'UI';

	// try to connect with amqp
	this.connect_();
};

/**
 * create connection to amqp
 * @private
 */
Receiver.prototype.createConnection_ = function() {
	return amqp.connect( this.amqpUrl_, this.connectOpt_ );
}

/**
 * create channel to amqp.
 * @param {Object} con Amqp connection
 * @private
 */
Receiver.prototype.createChannel_ = function( con ) {
	this.con_ = con;
	return this.con_.createConfirmChannel();
};

/**
 * create exchange for amqp in direct way.
 * @param {Object} ch Amqp channel
 * @private
 */
Receiver.prototype.createExchange_ = function( ch ) {
	this.ch_ = ch;
	return this.ch_.assertExchange( this.ex_ , 'direct', this.exchangeOpt_ );
};

/**
 * create queue for amqp
 * @private
 */
Receiver.prototype.createQueue_ = function() {
	return this.ch_.assertQueue( this.queueName_, this.queueOpt_ );
};

/**
 * bind queue for amqp
 * @private
 */
Receiver.prototype.bindQueue_ = function( qok ) {
	var queue = qok.queue;
	return this.ch_.bindQueue( queue, this.ex_, this.routingKey_ ) //this.queueName_ )
		.then(function() {
			return queue;
		});
};

Receiver.prototype.setPrefetchCnt_ = function() {
	// the global is false by default
	return this.ch_.prefetch(this.prefetchCount_, false);
}

/**
 * set up consume callback for amqp
 * @param {Object} queue Message queue in amqp
 * @private
 */
Receiver.prototype.consume_ = function( queue ) {
	return this.ch_.consume( queue, this.cb_, this.consumerOpt_ );
};

/**
 * handle connection error
 * @param {Object} con Amqp connection
 * @return {Object} The amqp connection
 * @private
 */
Receiver.prototype.handleConnectionError_ = function( con ) {
	this.con_ = con;

	this.con_.on('error', (function( that ) {
		return function( err ) {
			that.con_.close();
			that.reconnect_( err );
		};
	})(this));

	return con;
};

/**
 * handle channel error
 * @param {Object} ch Amqp channel
 * @return {Object} The amqp channel
 * @private
 */
Receiver.prototype.handleChannelError_ = function( ch ) {
	this.ch_ = ch;

	this.ch_.on('error', (function( that ) {
		return function( err ) {
			that.ch_.close();
			that.reconnect_( err );
		}
	})(this));

	return ch;
}


/**
 * try to reconnect to amqp
 * @private
 */
Receiver.prototype.reconnect_ = function( err ) {
	return when( 'retry' )
		.with( this )
		.delay( this.retryDelay_ )
		.then( function() {
			return this.connect_();
		})
		.catch( function( e ) {
			return this.connect_();
		});
};

/**
 * connect to amqp
 * @private
 */
Receiver.prototype.connect_ = function() {
	return when( this.createConnection_() )
		.with( this )
		.then( this.handleConnectionError_ )
		.then( this.createChannel_ )
		.then( this.handleChannelError_)
		.then( this.createExchange_ )
		.then( this.setPrefetchCnt_ )
		.then( this.createQueue_ )
		.then( this.bindQueue_ )
		.then( this.consume_ )
		.catch( function( err ) {
			return this.reconnect_( err );
		});
};

/**
 * exports
 */
module.exports = exports = Receiver;
