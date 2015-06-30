﻿using paramore.brighter.commandprocessor.Logging;

namespace paramore.brighter.commandprocessor.messaginggateway.awssqs
{
    public class SqsMessageConsumerFactory : IAmAMessageConsumerFactory
    {
        private readonly ILog _logger;

        public SqsMessageConsumerFactory(ILog logger)
        {
            _logger = logger;
        }

        public IAmAMessageConsumer Create(string queueName, string routingKey)
        {
            return new SqsMessageConsumer(queueName, _logger);
        }
    }
}