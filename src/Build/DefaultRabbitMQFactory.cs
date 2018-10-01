using PipServices.RabbitMQ.Queues;
using PipServices.Components.Build;
using PipServices.Commons.Refer;

namespace PipServices.RabbitMQ.Build
{
    /// <summary>
    /// Creates RabbitMQMessageQueue components by their descriptors.
    /// </summary>
    /// See <see cref="RabbitMQMessageQueue"/>
    public class DefaultRabbitMQFactory: Factory
    {
        public static Descriptor Descriptor = new Descriptor("pip-services", "factory", "rabbitmq", "default", "1.0");
        public static Descriptor RabbitMQMessageQueueFactoryDescriptor = new Descriptor("pip-services", "factory", "message-queue", "rabbitmq", "1.0");
        public static Descriptor RabbitMQMessageQueueDescriptor = new Descriptor("pip-services", "message-queue", "rabbitmq", "*", "1.0");

        /// <summary>
        /// Create a new instance of the factory.
        /// </summary>
        public DefaultRabbitMQFactory()
        {
            RegisterAsType(RabbitMQMessageQueueFactoryDescriptor, typeof(RabbitMQMessageQueueFactory));
            RegisterAsType(RabbitMQMessageQueueDescriptor, typeof(RabbitMQMessageQueue));
        }
    }
}
