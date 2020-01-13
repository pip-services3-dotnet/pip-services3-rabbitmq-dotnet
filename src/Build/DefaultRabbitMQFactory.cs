using PipServices3.RabbitMQ.Queues;
using PipServices3.Components.Build;
using PipServices3.Commons.Refer;

namespace PipServices3.RabbitMQ.Build
{
    /// <summary>
    /// Creates RabbitMQMessageQueue components by their descriptors.
    /// </summary>
    /// See <a href="https://rawgit.com/pip-services3-dotnet/pip-services3-rabbitmq-dotnet/master/doc/api/class_pip_services_1_1_rabbit_m_q_1_1_queues_1_1_rabbit_m_q_message_queue.html">RabbitMQMessageQueue</a>
    public class DefaultRabbitMQFactory: Factory
    {
        public static Descriptor Descriptor = new Descriptor("pip-services", "factory", "rabbitmq", "default", "1.0");
        public static Descriptor Descriptor3 = new Descriptor("pip-services3", "factory", "rabbitmq", "default", "1.0");
        public static Descriptor RabbitMQMessageQueueFactoryDescriptor = new Descriptor("pip-services", "factory", "message-queue", "rabbitmq", "1.0");
        public static Descriptor RabbitMQMessageQueueFactory3Descriptor = new Descriptor("pip-services3", "factory", "message-queue", "rabbitmq", "1.0");
        public static Descriptor RabbitMQMessageQueueDescriptor = new Descriptor("pip-services", "message-queue", "rabbitmq", "*", "1.0");
        public static Descriptor RabbitMQMessageQueue3Descriptor = new Descriptor("pip-services3", "message-queue", "rabbitmq", "*", "1.0");

        /// <summary>
        /// Create a new instance of the factory.
        /// </summary>
        public DefaultRabbitMQFactory()
        {
            RegisterAsType(RabbitMQMessageQueueFactoryDescriptor, typeof(RabbitMQMessageQueueFactory));
            RegisterAsType(RabbitMQMessageQueueFactory3Descriptor, typeof(RabbitMQMessageQueueFactory));
            RegisterAsType(RabbitMQMessageQueueDescriptor, typeof(RabbitMQMessageQueue));
            RegisterAsType(RabbitMQMessageQueue3Descriptor, typeof(RabbitMQMessageQueue));
        }
    }
}
