using System;
using System.Collections.Generic;

using Amazon.SQS.Model;

namespace paramore.brighter.commandprocessor.messaginggateway.awssqs
{
    public static class MessageExtensions
    {
        public static Dictionary<string, MessageAttributeValue> GenerateSqsMessageAttributes(this Message message)
        {
            var attributes = new Dictionary<string, MessageAttributeValue>()
                             {
                                 {SqsMessageAttributes.HANDLED_COUNT, new MessageAttributeValue(){DataType = "Number", StringValue = message.Header.HandledCount.ToString()}},
                                 {SqsMessageAttributes.TOPIC, new MessageAttributeValue{DataType = "String", StringValue = message.Header.Topic}},
                                 {SqsMessageAttributes.MESSAGE_ID, new MessageAttributeValue{DataType = "String", StringValue = message.Header.Id.ToString()}},
                                 {SqsMessageAttributes.MESSAGE_TYPE, new MessageAttributeValue{DataType = "String", StringValue = message.Header.MessageType.ToString()}},
                                 {SqsMessageAttributes.TIMESTAMP, new MessageAttributeValue{DataType = "String", StringValue = UnixTimestamp.GetUnixTimestampSeconds(message.Header.TimeStamp).ToString()}}
            };

            foreach (var bagValue in message.Header.Bag)
            {
                attributes.Add(bagValue.Key, new MessageAttributeValue { StringValue = bagValue.Value.ToString() });
            }

            return attributes;
        }

        public static Message ToMessage(this Amazon.SQS.Model.Message responseMessage)
        {
            var topic = responseMessage.MessageAttributes.ContainsKey(SqsMessageAttributes.TOPIC) ? responseMessage.MessageAttributes[SqsMessageAttributes.TOPIC].StringValue : string.Empty;
            var messageId = new Guid(responseMessage.MessageId);
            var messageType = responseMessage.MessageAttributes.ContainsKey(SqsMessageAttributes.MESSAGE_TYPE) ? (MessageType)Enum.Parse(typeof(MessageType), responseMessage.MessageAttributes[SqsMessageAttributes.MESSAGE_TYPE].StringValue) : MessageType.MT_EVENT;
            var handledCount = responseMessage.MessageAttributes.ContainsKey(SqsMessageAttributes.HANDLED_COUNT) ? int.Parse(responseMessage.MessageAttributes[SqsMessageAttributes.HANDLED_COUNT].StringValue) : 0;
            var timeStamp = responseMessage.MessageAttributes.ContainsKey(SqsMessageAttributes.TIMESTAMP) ? UnixTimestamp.DateTimeFromUnixTimestampSeconds(long.Parse(responseMessage.MessageAttributes[SqsMessageAttributes.TIMESTAMP].StringValue)) : DateTime.UtcNow;
            var receiptHandle = responseMessage.ReceiptHandle;

            var header = new MessageHeader(messageId, topic, messageType, timeStamp, handledCount);
            header.Bag.Add(SqsMessageAttributes.RECEIPT_HANDLE, receiptHandle);

            return new Message(header, new MessageBody(responseMessage.Body));
        }
    }
}