using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PipServices.Commons.Config;
using PipServices.Commons.Convert;
using PipServices.Commons.Errors;
using PipServices.Components.Auth;
using PipServices.Components.Connect;
using PipServices.Messaging.Queues;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PipServices.RabbitMQ.Queues
{
    public class RabbitMQMessageQueue : MessageQueue
    {
        //private long DefaultVisibilityTimeout = 60000;
        private long DefaultCheckInterval = 10000;

        private IConnection _connection;
        private IModel _model;
        private string _queue;
        private string _exchange;
        private bool _persistent;
        private string _routingKey;
        private CancellationTokenSource _cancel = new CancellationTokenSource();

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

        public override void Configure(ConfigParams config)
        {
            base.Configure(config);

            Interval = config.GetAsLongWithDefault("interval", Interval);
        }

        private void CheckOpened(string correlationId)
        {
            if (_model == null)
                throw new InvalidStateException(correlationId, "NOT_OPENED", "The queue is not opened");
        }

        public override bool IsOpen()
        {
            return _model != null && _model.IsOpen;
        }

        public async override Task OpenAsync(string correlationId, ConnectionParams connection, CredentialParams credential)
        {
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
            _queue = connection.GetAsStringWithDefault("queue", _queue);
            _exchange = connection.GetAsStringWithDefault("exchange", "");
            _persistent = connection.GetAsBoolean("persistent");
            _routingKey = connection.GetAsStringWithDefault("routing_key", "");
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
            if (connection.GetAsBoolean("auto_create"))
            {
                if (!string.IsNullOrEmpty(_exchange))
                {
                    _model.ExchangeDeclare(
                        _exchange,
                        connection.GetAsStringWithDefault("exchange_type", "fanout"),
                        connection.GetAsBoolean("persistent"),
                        connection.GetAsBoolean("auto_delete"),
                        null
                    );
                }

                if (!connection.GetAsBoolean("no_queue"))
                {
                    if (string.IsNullOrEmpty(_queue))
                    {
                        _queue = _model.QueueDeclare(
                            "",
                            connection.GetAsBoolean("persistent"),
                            true,
                            true
                        ).QueueName;
                    }
                    else
                    {
                        _model.QueueDeclare(
                            _queue,
                            connection.GetAsBoolean("persistent"),
                            connection.GetAsBoolean("exclusive"),
                            connection.GetAsBoolean("auto_delete"),
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

        public override long? MessageCount
        {
            get
            {
                CheckOpened(null);

                if (string.IsNullOrEmpty(_queue))
                    return 0;

                return _model.MessageCount(_queue);
            }
        }

        private MessageEnvelope ToMessage(BasicGetResult envelope)
        {
            if (envelope == null) return null;

            MessageEnvelope message = new MessageEnvelope
            {
                MessageId = envelope.BasicProperties.MessageId,
                MessageType = envelope.BasicProperties.Type,
                CorrelationId = envelope.BasicProperties.CorrelationId,
                Message = Encoding.UTF8.GetString(envelope.Body),
                SentTime = DateTime.UtcNow,
                Reference = envelope
            };

            return message;
        }

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

            var messageBuffer = Encoding.UTF8.GetBytes(message.Message);

            _model.BasicPublish(_exchange, _routingKey, properties, messageBuffer);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via {1}", message, this);

            await Task.Delay(0);
        }

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

        public override async Task RenewLockAsync(MessageEnvelope message, long lockTimeout)
        {
            CheckOpened(message.CorrelationId);

            // Operation is not supported

            await Task.Delay(0);
        }

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

        public override async Task MoveToDeadLetterAsync(MessageEnvelope message)
        {
            CheckOpened(message.CorrelationId);

            // Operation is not supported

            await Task.Delay(0);
        }

        public override async Task ListenAsync(string correlationId, Func<MessageEnvelope, IMessageQueue, Task> callback)
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
                        await callback(message, this);
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

        public override void EndListen(string correlationId)
        {
            _cancel.Cancel();
        }

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
