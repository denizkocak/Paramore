using System.Linq;

using Amazon.SQS;

namespace paramore.commandprocessor.tests.MessagingGateway.awssqs
{
    public class TestAWSQueueListener
    {
        private string _queueUrl;
        private AmazonSQSClient _client;

        public TestAWSQueueListener(string queueUrl)
        {
            _queueUrl = queueUrl;
            _client = new AmazonSQSClient();
        }

        public string Listen()
        {
            var response = _client.ReceiveMessage(_queueUrl);
            if (!response.Messages.Any()) return null;

            return response.Messages.First().Body;
        }

        public void Purge(string queueUrl)
        {
            _client.PurgeQueue(_queueUrl);
        }
    }
}