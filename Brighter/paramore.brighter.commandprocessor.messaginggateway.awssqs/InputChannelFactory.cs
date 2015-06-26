namespace paramore.brighter.commandprocessor.messaginggateway.awssqs
{
    public class InputChannelFactory : IAmAChannelFactory
    {
        private readonly SqsMessageConsumerFactory _messageConsumerFactory;

        /// <summary>
        /// Initializes a new instance of the <see cref="InputChannelFactory"/> class.
        /// </summary>
        /// <param name="messageConsumerFactory">The messageConsumerFactory.</param>
        public InputChannelFactory(SqsMessageConsumerFactory messageConsumerFactory)
        {
            _messageConsumerFactory = messageConsumerFactory;
        }

        /// <summary>
        /// Creates the input channel.
        /// </summary>
        /// <param name="channelName">Name of the channel.</param>
        /// <param name="routingKey">The routing key.</param>
        /// <returns>IAmAnInputChannel.</returns>
        public IAmAnInputChannel CreateInputChannel(string channelName, string routingKey)
        {
            return new InputChannel(channelName, _messageConsumerFactory.Create(channelName, routingKey));
        }

        /// <summary>
        /// Creates the output channel.
        /// </summary>
        /// <param name="channelName">Name of the channel.</param>
        /// <param name="routingKey">The routing key.</param>
        /// <returns>IAmAnInputChannel.</returns>
        public IAmAnInputChannel CreateOutputChannel(string channelName, string routingKey)
        {
            return new InputChannel(channelName, _messageConsumerFactory.Create(channelName, routingKey));
        }
    }
}