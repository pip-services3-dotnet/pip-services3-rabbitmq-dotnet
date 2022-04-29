using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using System;
using Xunit;

namespace PipServices3.RabbitMQ.Queues
{
    [Collection("Sequential")]
    public class RabbitMQMessageQueueTest: IDisposable
    {
        private bool _enabled;
        private RabbitMQMessageQueue _queue;
        private MessageQueueFixture _fixture;

        public RabbitMQMessageQueueTest()
        {
            var RABBITMQ_ENABLED = Environment.GetEnvironmentVariable("RABBITMQ_ENABLED") ?? "true";
            var RABBITMQ_URI = Environment.GetEnvironmentVariable("RABBITMQ_URI");
            var RABBITMQ_HOST = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";
            var RABBITMQ_PORT = Environment.GetEnvironmentVariable("RABBITMQ_PORT") ?? "5672";
            var RABBITMQ_QUEUE = Environment.GetEnvironmentVariable("RABBITMQ_QUEUE") ?? "test";
            var RABBITMQ_EXCHANGE = Environment.GetEnvironmentVariable("RABBITMQ_EXCHANGE") ?? "test";
            var RABBITMQ_USER = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "user";
            var RABBITMQ_PASS = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "pass123";

            _enabled = BooleanConverter.ToBoolean(RABBITMQ_ENABLED);

            if (_enabled)
            {
                _queue = new RabbitMQMessageQueue("TestQueue");
                _queue.Configure(ConfigParams.FromTuples(
                    "exchange", RABBITMQ_EXCHANGE,
                    "queue", RABBITMQ_QUEUE,
                    "options.auto_create", true,
                    "connection.uri", RABBITMQ_URI,
                    "connection.host", RABBITMQ_HOST,
                    "connection.port", RABBITMQ_PORT,
                    "credential.username", RABBITMQ_USER,
                    "credential.password", RABBITMQ_PASS
                ));
                _queue.Interval = 100;

                _queue.OpenAsync(null).Wait();
                _queue.ClearAsync(null).Wait();

                _fixture = new MessageQueueFixture(_queue);
            }
        }

        public void Dispose()
        {
            if (_queue != null)
                _queue.CloseAsync(null).Wait();
        }

        [Fact]
        public void TestRabbitMQSendReceiveMessage()
        {
            if (_enabled)
                _fixture.TestSendReceiveMessageAsync().Wait();
        }

        [Fact]
        public void TestRabbitMQReceiveSendMessage()
        {
            if (_enabled)
                _fixture.TestReceiveSendMessageAsync().Wait();
        }

        [Fact]
        public void TestRabbitMQReceiveAndComplete()
        {
            if (_enabled)
                _fixture.TestReceiveAndCompleteMessageAsync().Wait();
        }

        [Fact]
        public void TestRabbitMQReceiveAndAbandon()
        {
            if (_enabled)
                _fixture.TestReceiveAndAbandonMessageAsync().Wait();
        }

        [Fact]
        public void TestRabbitMQSendPeekMessage()
        {
            if (_enabled)
                _fixture.TestSendPeekMessageAsync().Wait();
        }

        [Fact]
        public void TestRabbitMQPeekNoMessage()
        {
            if (_enabled)
                _fixture.TestPeekNoMessageAsync().Wait();
        }

        [Fact]
        public void TestRabbitMQOnMessage()
        {
            if (_enabled)
                _fixture.TestOnMessageAsync().Wait();
        }

        //[Fact]
        //public void TestRabbitMQMoveToDeadMessage()
        //{
        //    if (_enabled)
        //        _fixture.TestMoveToDeadMessageAsync().Wait();
        //}

        [Fact]
        public void TestRabbitMQMessageCount()
        {
            if (_enabled)
                _fixture.TestMessageCountAsync().Wait();
        }
    }
}
