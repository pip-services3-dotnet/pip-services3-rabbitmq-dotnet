using PipServices3.Components.Build;
using PipServices3.Commons.Config;
using PipServices3.Commons.Refer;

namespace PipServices3.RabbitMQ.Queues
{
    public class RabbitMQMessageQueueFactory : Factory, IConfigurable
    {
        public static readonly Descriptor Descriptor = new Descriptor("pip-services3-rabbitmq", "factory", "message-queue", "rabbitmq", "1.0");
        public static readonly Descriptor MemoryQueueDescriptor = new Descriptor("pip-services3-rabbitmq", "message-queue", "rabbitmq", "*", "*");

        private ConfigParams _config;

        public RabbitMQMessageQueueFactory()
        {
            Register(MemoryQueueDescriptor, (locator) => {
                Descriptor descriptor = (Descriptor)locator;
                var queue = new RabbitMQMessageQueue(descriptor.Name);
                if (_config != null)
                    queue.Configure(_config);
                return queue;
            });
        }

        public void Configure(ConfigParams config)
        {
            _config = config;
        }
    }
}
