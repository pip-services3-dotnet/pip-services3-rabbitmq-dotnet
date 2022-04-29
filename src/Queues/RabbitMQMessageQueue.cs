using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using PipServices3.Commons.Errors;
using PipServices3.Components.Auth;
using PipServices3.Components.Connect;
using PipServices3.Messaging.Queues;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PipServices3.RabbitMQ.Queues
{
    /// <summary>
    /// Message queue that sends and receives messages via MQTT message broker.
    /// 
    /// MQTT is a popular light-weight protocol to communicate IoT devices.
    /// 
    /// ### Configuration parameters ###
    /// 
    /// - queue:                        name of RabbitMQ queue to subscribe
    /// - intervatl:                    wait for check message interval
    /// - exchange:                     RabbitMQ exchange name
    ///
    /// connection(s):
    /// - discovery_key:               (optional) a key to retrieve the connection from <a href="https://pip-services3-dotnet.github.io/pip-services3-components-dotnet/interface_pip_services_1_1_components_1_1_connect_1_1_i_discovery.html">IDiscovery</a>
    /// - host:                        host name or IP address
    /// - port:                        port number
    /// - uri:                         resource URI or connection string with all parameters in it
    /// 
    /// credential(s):
    /// - store_key:                   (optional) a key to retrieve the credentials from <a href="https://pip-services3-dotnet.github.io/pip-services3-components-dotnet/interface_pip_services_1_1_components_1_1_auth_1_1_i_credential_store.html">ICredentialStore</a>
    /// - username:                    user name
    /// - password:                    user password
    /// 
    /// options(s):
    /// - exchange_type:                (optional) RabbitMQ exchange type (default: fanout)
    /// - routing_key:                  (optional) RabbitMQ routing key
    /// - persistent:                   (optional) is persistent messages (default: false)
    /// - exclusive:                    (optional) is exclusive queue (default: false)
    /// - auto_create:                  (optional) autocreation queue (default: false)
    /// - auto_delete:                  (optional) The queue is automatically deleted when the last consumer unsubscribes (default: false)
    /// - no_queue:                     (optional) autogenerate queue name (default: false)
    ///
    /// ### References ###
    /// 
    /// - *:logger:*:*:1.0             (optional) <a href="https://pip-services3-dotnet.github.io/pip-services3-components-dotnet/interface_pip_services_1_1_components_1_1_log_1_1_i_logger.html">ILogger</a> components to pass log messages
    /// - *:counters:*:*:1.0           (optional) <a href="https://pip-services3-dotnet.github.io/pip-services3-components-dotnet/interface_pip_services_1_1_components_1_1_count_1_1_i_counters.html">ICounters</a> components to pass collected measurements
    /// - *:discovery:*:*:1.0          (optional) <a href="https://pip-services3-dotnet.github.io/pip-services3-components-dotnet/interface_pip_services_1_1_components_1_1_connect_1_1_i_discovery.html">IDiscovery</a> services to resolve connections
    /// - *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials
    /// </summary>
    /// <example>
    /// <code>
    /// var queue = new RabbitMQMessageQueue("myqueue");
    /// queue.configure(ConfigParams.FromTuples(
    /// "queue", "mytopic",
    /// "connection.protocol", "mqtt"
    /// "connection.host", "localhost"
    /// "connection.port", 1883 ));
    /// queue.Open("123");
    /// 
    /// queue.Send("123", new MessageEnvelop(null, "mymessage", "ABC"));
    /// queue.Receive("123", 0);
    /// queue.Complete("123", message);
    /// </code>
    /// </example>
    public class RabbitMQMessageQueue : MessageQueue
    {
        //private long DefaultVisibilityTimeout = 60000;
        private long DefaultCheckInterval = 10000;

        private IConnection _connection;
        private IModel _model;
        private string _queue;
        private string _exchange = "";
        private string _exchangeType = "fanout";
        private string _routingKey = "";
        private bool _persistent = false;
        private bool _exclusive = false;
        private bool _autoCreate = false;
        private bool _autoDelete = false;
        private bool _noQueue = false;
        private CancellationTokenSource _cancel = new CancellationTokenSource();

        /// <summary>
        /// Creates a new instance of the message queue.
        /// </summary>
        /// <param name="name">(optional) a queue name.</param>
        public RabbitMQMessageQueue(string name = null)
        {
            Name = name;
            Capabilities = new MessagingCapabilities(true, true, true, true, true, false, true, false, true);
            Interval = DefaultCheckInterval;
        }

        public RabbitMQMessageQueue(string name, ConfigParams config)
            : this(name)
        {
            if (config != null) Configure(config);
        }

        public RabbitMQMessageQueue(string name, IModel model, string queue)
            : this(name)
        {
            _model = model;
            _queue = queue;
        }

        public long Interval { get; set; }

        /// <summary>
        /// Configures component by passing configuration parameters.
        /// </summary>
        /// <param name="config">configuration parameters to be set.</param>
        public override void Configure(ConfigParams config)
        {
            base.Configure(config);

            Interval = config.GetAsLongWithDefault("interval", Interval);

            _queue = config.GetAsStringWithDefault("queue", _queue ?? Name);
            _exchange = config.GetAsStringWithDefault("exchange", _exchange);

            _exchangeType = config.GetAsStringWithDefault("options.exchange_type", _exchangeType);
            _routingKey = config.GetAsStringWithDefault("options.routing_key", _routingKey);
            _persistent = config.GetAsBooleanWithDefault("options.persistent", _persistent);
            _exclusive = config.GetAsBooleanWithDefault("options.exclusive", _exclusive);
            _autoCreate = config.GetAsBooleanWithDefault("options.auto_create", _autoCreate);            
            _autoDelete = config.GetAsBooleanWithDefault("options.auto_delete", _autoDelete);            
            _noQueue = config.GetAsBooleanWithDefault("options.no_queue", _noQueue);            
        }

        private void CheckOpened(string correlationId)
        {
            if (_model == null)
                throw new InvalidStateException(correlationId, "NOT_OPENED", "The queue is not opened");
        }

        /// <summary>
        /// Checks if the component is opened.
        /// </summary>
        /// <returns>true if the component has been opened and false otherwise.</returns>
        public override bool IsOpen()
        {
            return _model != null && _model.IsOpen;
        }

        /// <summary>
        /// Opens the component with given connection and credential parameters.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="connections">connection parameters</param>
        /// <param name="credential">credential parameters</param>
        public override async Task OpenAsync(string correlationId, List<ConnectionParams> connections, CredentialParams credential)
        {
            var connection = connections?.FirstOrDefault();
            if (connection == null)
            {
                throw new ArgumentNullException(nameof(connections));
            }

            var connectionFactory = new ConnectionFactory();

            if (!string.IsNullOrEmpty(connection.Uri))
            {
                var uri = new Uri(connection.Uri);
                connectionFactory.Uri = uri;
            }
            else
            {
                connectionFactory.HostName = connection.Host;
                connectionFactory.Port = connection.Port != 0 ? connection.Port : 5672;
            }

            if (credential != null && !string.IsNullOrEmpty(credential.Username))
            {
                //if (!string.IsNullOrEmpty(connection.Protocol))
                    //connectionFactory.Protocol = Protocols.DefaultProtocol;
                connectionFactory.UserName = credential.Username;
                connectionFactory.Password = credential.Password;
            }

            try
            {
                _connection = connectionFactory.CreateConnection();
            }
            catch (Exception ex)
            {
                var uri = connection.Uri;
                if (string.IsNullOrEmpty(uri))
                    uri = $"rabbitmq://{connectionFactory.HostName}:{connectionFactory.Port}";
                
                throw new ConnectionException(
                    correlationId, 
                    "CANNOT_CONNECT", 
                    "Cannot connect to RabbitMQ at " + uri
                ).Wrap(ex);
            }

            _model = _connection.CreateModel();
            _cancel = new CancellationTokenSource();

            if (string.IsNullOrEmpty(_queue) && string.IsNullOrEmpty(_exchange))
            {
                throw new ConfigException(
                    correlationId,
                    "NO_QUEUE",
                    "Queue or exchange are not defined in connection parameters"
                );
            }

            // Automatically create queue, exchange and binding
            if (_autoCreate)
            {
                if (!string.IsNullOrEmpty(_exchange))
                {
                    _model.ExchangeDeclare(
                        _exchange,
                        _exchangeType,
                        _persistent,
                        _autoDelete,
                        null
                    );
                }

                if (!connection.GetAsBoolean("no_queue"))
                {
                    if (string.IsNullOrEmpty(_queue))
                    {
                        _queue = _model.QueueDeclare(
                            "",
                            _persistent,
                            true,
                            true
                        ).QueueName;
                    }
                    else
                    {
                        _model.QueueDeclare(
                            _queue,
                            _persistent,
                            _exclusive,
                            _autoDelete,
                            null
                        );
                    }

                    if (!string.IsNullOrEmpty(_exchange))
                    {
                        _model.QueueBind(
                            _queue,
                            _exchange,
                            _routingKey
                        );
                    }
                }
            }

            await Task.Delay(0); 
        }

        /// <summary>
        /// Closes component and frees used resources.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public override async Task CloseAsync(string correlationId)
        {
            var model = _model;
            if (model != null && model.IsOpen)
                model.Close();

            var connection = _connection;
            if (connection != null && connection.IsOpen)
                connection.Close();

            _connection = null;
            _model = null;

            _cancel.Cancel();

            _logger.Trace(correlationId, "Closed queue {0}", this);

            await Task.Delay(0);
        }

        public override async Task<long> ReadMessageCountAsync()
        {
                CheckOpened(null);

                return string.IsNullOrEmpty(_queue) ? 0 : _model.MessageCount(_queue);
        }

        private MessageEnvelope ToMessage(BasicGetResult envelope)
        {
            if (envelope == null) return null;

            MessageEnvelope message = new MessageEnvelope
            {
                MessageId = envelope.BasicProperties.MessageId,
                MessageType = envelope.BasicProperties.Type,
                CorrelationId = envelope.BasicProperties.CorrelationId,
                Message = envelope.Body.Span.ToArray(),
                SentTime = DateTime.UtcNow,
                Reference = envelope
            };

            return message;
        }

        /// <summary>
        /// Sends a message into the queue.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="message">a message envelop to be sent.</param>
        public override async Task SendAsync(string correlationId, MessageEnvelope message)
        {
            CheckOpened(correlationId);
            var content = JsonConverter.ToJson(message);

            var properties = _model.CreateBasicProperties();
            if (!string.IsNullOrEmpty(message.CorrelationId))
                properties.CorrelationId = message.CorrelationId;
            if (!string.IsNullOrEmpty(message.MessageId))
                properties.MessageId = message.MessageId;
            properties.Persistent = _persistent;
            if (!string.IsNullOrEmpty(message.MessageType))
                properties.Type = message.MessageType;

            var messageBuffer = message.Message;

            _model.BasicPublish(_exchange, _routingKey, properties, messageBuffer);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via {1}", message, this);

            await Task.Delay(0);
        }

        /// <summary>
        /// Peeks a single incoming message from the queue without removing it.
        /// If there are no messages available in the queue it returns null.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <returns>a message</returns>
        public override async Task<MessageEnvelope> PeekAsync(string correlationId)
        {
            CheckOpened(correlationId);

            var envelope = _model.BasicGet(_queue, false);
            if (envelope == null) return null;

            var message = ToMessage(envelope);
            if (message != null)
            {
                _logger.Trace(message.CorrelationId, "Peeked message {0} on {1}", message, this);
            }

            return await Task.FromResult<MessageEnvelope>(message);
        }

        /// <summary>
        /// Peeks multiple incoming messages from the queue without removing them.
        /// If there are no messages available in the queue it returns an empty list.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="messageCount">a maximum number of messages to peek.</param>
        /// <returns>a list with messages</returns>
        public override async Task<List<MessageEnvelope>> PeekBatchAsync(string correlationId, int messageCount)
        {
            CheckOpened(correlationId);

            var messages = new List<MessageEnvelope>();

            while (messageCount > 0)
            {
                var envelope = _model.BasicGet(_queue, false);
                if (envelope == null)
                    break;

                var message = ToMessage(envelope);
                messages.Add(message);
                messageCount--;
            }

            _logger.Trace(correlationId, "Peeked {0} messages on {1}", messages.Count, this);

            return await Task.FromResult<List<MessageEnvelope>>(messages);
        }

        /// <summary>
        /// Receives an incoming message and removes it from the queue.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="waitTimeout">a timeout in milliseconds to wait for a message to come.</param>
        /// <returns>a message</returns>
        public override async Task<MessageEnvelope> ReceiveAsync(string correlationId, long waitTimeout)
        {
            BasicGetResult envelope = null;

            do
            {
                // Read the message and exit if received
                envelope = _model.BasicGet(_queue, false);
                if (envelope != null) break;
                if (waitTimeout <= 0) break;

                // Wait for check interval and decrement the counter
                await Task.Delay(TimeSpan.FromMilliseconds(Interval));
                waitTimeout = waitTimeout - Interval;
                if (waitTimeout <= 0) break;
            }
            while (!_cancel.Token.IsCancellationRequested);

            var message = ToMessage(envelope);

            if (message != null)
            {
                _counters.IncrementOne("queue." + Name + ".received_messages");
                _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);
            }

            return await Task.FromResult<MessageEnvelope>(message);
        }

        /// <summary>
        /// Renews a lock on a message that makes it invisible from other receivers in the queue.
        /// This method is usually used to extend the message processing time.
        /// 
        /// Important: This method is not supported by MQTT.
        /// </summary>
        /// <param name="message">a message to extend its lock.</param>
        /// <param name="lockTimeout">a locking timeout in milliseconds.</param>
        public override async Task RenewLockAsync(MessageEnvelope message, long lockTimeout)
        {
            CheckOpened(message.CorrelationId);

            // Operation is not supported

            await Task.Delay(0);
        }

        /// <summary>
        /// Returnes message into the queue and makes it available for all subscribers to receive it again.
        /// This method is usually used to return a message which could not be processed at the moment
        /// to repeat the attempt.Messages that cause unrecoverable errors shall be removed permanently
        /// or/and send to dead letter queue.
        /// 
        /// Important: This method is not supported by MQTT.
        /// </summary>
        /// <param name="message">a message to return.</param>
        public override async Task AbandonAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            // Make the message immediately visible
            var envelope = (BasicGetResult) message.Reference;
            if (envelope != null)
            {
                _model.BasicNack(envelope.DeliveryTag, false, true);

                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Abandoned message {0} at {1}", message, this);
            }

            await Task.Delay(0);
        }

        /// <summary>
        /// Permanently removes a message from the queue.
        /// This method is usually used to remove the message after successful processing.
        /// 
        /// Important: This method is not supported by MQTT.
        /// </summary>
        /// <param name="message">a message to remove.</param>
        public override async Task CompleteAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            var envelope = (BasicGetResult)message.Reference;
            if (envelope != null)
            {
                _model.BasicAck(envelope.DeliveryTag, false);

                message.Reference = null;
                _logger.Trace(message.CorrelationId, "Completed message {0} at {1}", message, this);
            }

            await Task.Delay(0);
        }

        /// <summary>
        /// Permanently removes a message from the queue and sends it to dead letter queue.
        /// 
        /// Important: This method is not supported by MQTT.
        /// </summary>
        /// <param name="message">a message to be removed.</param>
        /// <returns></returns>
        public override async Task MoveToDeadLetterAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            // Operation is not supported

            await Task.Delay(0);
        }

        /// <summary>
        /// Listens for incoming messages and blocks the current thread until queue is closed.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="receiver"></param>
        /// <returns></returns>
        public override async Task ListenAsync(string correlationId, IMessageReceiver receiver)
        {
            CheckOpened(correlationId);
            _logger.Debug(correlationId, "Started listening messages at {0}", this);

            // Create new cancelation token
            _cancel = new CancellationTokenSource();

            while (!_cancel.IsCancellationRequested)
            {
                var envelope = _model.BasicGet(_queue, false);

                if (envelope != null && !_cancel.IsCancellationRequested)
                {
                    var message = ToMessage(envelope);

                    _counters.IncrementOne("queue." + Name + ".received_messages");
                    _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, this);

                    try
                    {
                        await receiver.ReceiveMessageAsync(message, this);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(correlationId, ex, "Failed to process the message");
                        //throw ex;
                    }
                }
                else
                {
                    // If no messages received then wait
                    await Task.Delay(TimeSpan.FromMilliseconds(Interval));
                }
            }

            await Task.Delay(0);
        }

        /// <summary>
        /// Ends listening for incoming messages.
        /// When this method is call listen unblocks the thread and execution continues.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public override void EndListen(string correlationId)
        {
            _cancel.Cancel();
        }

        /// <summary>
        /// Clears component state.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <returns></returns>
        public override async Task ClearAsync(string correlationId)
        {
            CheckOpened(correlationId);

            if (!string.IsNullOrEmpty(_queue))
                _model.QueuePurge(_queue);

            _logger.Trace(null, "Cleared queue {0}", this);

            await Task.Delay(0);
        }
    }
}
