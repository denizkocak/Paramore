using System.Threading.Tasks;

using Amazon.SQS;
using Amazon.SQS.Model;

using Newtonsoft.Json;

using paramore.brighter.commandprocessor.Logging;

namespace paramore.brighter.commandprocessor.messaginggateway.awssqs
{
    public class SqsMessageProducer : IAmAMessageProducer
    {
        private readonly ILog _logger;
        private readonly string _queueUrl;

        public SqsMessageProducer(string queueUrl, ILog logger)
        {
            _logger = logger;
            _queueUrl = queueUrl;
        }

        public Task Send(Message message)
        {
            _logger.DebugFormat("SQSMessageProducer: Publishing message to queue {0} with topic {1} and id {2} and message: {3}", _queueUrl, message.Header.Topic, message.Id, JsonConvert.SerializeObject(message));

            var messageAttributes = message.GenerateSqsMessageAttributes();

            var sendMessageRequest = new SendMessageRequest { MessageBody = message.Body.Value, QueueUrl = _queueUrl, MessageAttributes = messageAttributes };

            using (var client = new AmazonSQSClient())
            {
                return client.SendMessageAsync(sendMessageRequest);                
            }
        }

        public void Dispose()
        {
            
        }
    }
}