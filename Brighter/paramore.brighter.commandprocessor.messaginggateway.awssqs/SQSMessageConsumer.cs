﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

using Amazon.SQS;
using Amazon.SQS.Model;

using Newtonsoft.Json;

using paramore.brighter.commandprocessor.Logging;

using TimeSpan = System.TimeSpan;

namespace paramore.brighter.commandprocessor.messaginggateway.awssqs
{
    public class SqsMessageConsumer : IAmAMessageConsumer
    {
        private readonly ILog _logger;
        private readonly string _queueUrl;

        public SqsMessageConsumer(string queueUrl, ILog logger)
        {
            _logger = logger;
            _queueUrl = queueUrl;
        }

        public Message Receive(int timeoutInMilliseconds)
        {
            _logger.DebugFormat("SqsMessageConsumer: Preparing to retrieve next message from queue {0}", _queueUrl);

            var message = new Message();
            var request = new ReceiveMessageRequest(_queueUrl)
                          {
                              MaxNumberOfMessages = 1,
                              WaitTimeSeconds = (int)TimeSpan.FromMilliseconds(timeoutInMilliseconds).TotalSeconds,
                              MessageAttributeNames = new List<string>
                                                      {
                                                          SqsMessageAttributes.TOPIC, 
                                                          SqsMessageAttributes.MESSAGE_ID, 
                                                          SqsMessageAttributes.MESSAGE_TYPE, 
                                                          SqsMessageAttributes.HANDLED_COUNT, 
                                                          SqsMessageAttributes.TIMESTAMP 
                                                      }
                          };

            using (var client = new AmazonSQSClient())
            {
                var response = client.ReceiveMessage(request);

                if (response.HttpStatusCode != HttpStatusCode.OK) 
                    return message;

                if(response.ContentLength == 0)
                    return message;

                if(!response.Messages.Any())
                    return message;

                message = response.Messages.First().ToMessage();

                _logger.InfoFormat("SqsMessageConsumer: Received message from queue {0}, message: {1}{2}",
                        _queueUrl, Environment.NewLine, JsonConvert.SerializeObject(message));
            }

            return message;
        }

        public void Acknowledge(Message message)
        {
            if(!message.Header.Bag.ContainsKey("ReceiptHandle"))
                return;

            var receiptHandle = message.Header.Bag["ReceiptHandle"].ToString();

            try
            {
                using (var client = new AmazonSQSClient())
                {
                    client.DeleteMessageAsync(new DeleteMessageRequest(_queueUrl, receiptHandle));

                    _logger.InfoFormat("SqsMessageConsumer: Deleted the message {0} with receipt handle {1} on the queue {2}", message.Id, receiptHandle, _queueUrl);
                }
            }
            catch (Exception exception)
            {
                _logger.ErrorException("SqsMessageConsumer: Error during deleting the message {0} with receipt handle {1} on the queue {2}", exception, message.Id, receiptHandle, _queueUrl);
                throw;
            }
        }

        public void Reject(Message message, bool requeue)
        {
            if (!message.Header.Bag.ContainsKey("ReceiptHandle"))
                return;

            var receiptHandle = message.Header.Bag["ReceiptHandle"].ToString();

            try
            {
                _logger.InfoFormat("SqsMessageConsumer: Rejecting the message {0} with receipt handle {1} on the queue {2} with requeue paramter {3}", message.Id, receiptHandle, _queueUrl, requeue);
                
                using (var client = new AmazonSQSClient())
                {
                    if (requeue)
                    {
                        client.ChangeMessageVisibility(new ChangeMessageVisibilityRequest(_queueUrl, receiptHandle, 0));
                    }
                    else
                    {
                        client.DeleteMessage(_queueUrl, receiptHandle);
                    }
                }

                _logger.InfoFormat("SqsMessageConsumer: Message {0} with receipt handle {1} on the queue {2} with requeue paramter {3} has been rejected", message.Id, receiptHandle, _queueUrl, requeue);
            }
            catch (Exception exception)
            {
                _logger.ErrorException("SqsMessageConsumer: Error during rejecting the message {0} with receipt handle {1} on the queue {2}", exception, message.Id, receiptHandle, _queueUrl);
                throw;
            }
        }

        public void Purge()
        {
            try
            {
                using (var client = new AmazonSQSClient())
                {
                    _logger.InfoFormat("SqsMessageConsumer: Purging the queue {0}", _queueUrl);

                    client.PurgeQueue(_queueUrl);

                    _logger.InfoFormat("SqsMessageConsumer: Purged the queue {0}", _queueUrl);
                }
            }
            catch (Exception exception)
            {
                _logger.ErrorException("SqsMessageConsumer: Error purging queue {0}", exception, _queueUrl);
                throw;
            }
        }

        public void Requeue(Message message)
        {
            try
            {
                using (var client = new AmazonSQSClient())
                {
                    _logger.InfoFormat("SqsMessageConsumer: requeueing the message {0}", message.Id);

                    client.SendMessage(_queueUrl, message.Body.Value);
                }

                Reject(message, false);

                _logger.InfoFormat("SqsMessageConsumer: requeued the message {0}", message.Id);
            }
            catch (Exception exception)
            {
                _logger.ErrorException("SqsMessageConsumer: Error purging queue {0}", exception, _queueUrl);
                throw;
            }
        }

        public void Dispose()
        {
            
        }
    }
}