using System;

namespace paramore.brighter.commandprocessor.messaginggateway.awssqs
{
    public class SqsMessage
    {
        public Guid MessageId { get; set; }

        public string Message { get; set; }
    }
}