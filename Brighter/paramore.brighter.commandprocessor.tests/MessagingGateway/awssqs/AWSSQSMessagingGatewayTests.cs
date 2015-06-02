﻿using System;
using System.Linq;

using Machine.Specifications;

using paramore.brighter.commandprocessor;
using paramore.brighter.commandprocessor.Logging;
using paramore.brighter.commandprocessor.messaginggateway.awssqs;
using paramore.brighter.commandprocessor.messaginggateway.rmq;

using Message = paramore.brighter.commandprocessor.Message;

namespace paramore.commandprocessor.tests.MessagingGateway.awssqs
{
    [Subject("Messaging Gateway")]
    [Tags("Requires", new[] { "AWSSDK" })]
    public class AWSSQSMessagingGatewayTests
    {
        public class When_posting_a_message_via_the_messaging_gateway
        {
            private Establish context = () =>
            {
                _queueListener = new TestAWSQueueListener(_queueUrl);
                var logger = LogProvider.For<RmqMessageConsumer>();
                _message = new Message(header: new MessageHeader(Guid.NewGuid(), "test1", MessageType.MT_COMMAND), body: new MessageBody("test content"));

                _messageProducer = new SqsMessageProducer(_queueUrl, logger);
            };

            private Because of = async () =>
            {
                var task = _messageProducer.Send(_message);
                
                task.ContinueWith(
                    x =>
                    {
                        if(x.IsCompleted)
                            _listenedMessage = _queueListener.Listen();        
                    }).Wait();
            };

            It should_send_the_message_to_aws_sqs = () => _listenedMessage.ShouldNotBeNull();

            private Cleanup queue = () => _queueListener.Purge(_queueUrl);

            private static Message _message;
            private static SqsMessageProducer _messageProducer;
            private static TestAWSQueueListener _queueListener;
            private static string _listenedMessage;
            private static string _queueUrl = "https://sqs.eu-west-1.amazonaws.com/027649620536/brighter-test-queue";
        }

        public class When_posting_a_message_via_the_messaging_gateway_and_queue_does_not_exist
        {
            private Establish context = () =>
            {
                _queueListener = new TestAWSQueueListener(_queueUrl);
                var logger = LogProvider.For<RmqMessageConsumer>();
                _message = new Message(header: new MessageHeader(Guid.NewGuid(), "test1", MessageType.MT_COMMAND), body: new MessageBody("test content"));

                _messageProducer = new SqsMessageProducer(_queueUrl, logger);
            };

            private Because of = async () =>
            {
                var task = _messageProducer.Send(_message);
                
                task.ContinueWith(
                    x =>
                    {
                        if(x.IsCompleted)
                            _listenedMessage = _queueListener.Listen();        
                    }).Wait();
            };

            It should_send_the_message_to_aws_sqs = () => _listenedMessage.ShouldNotBeNull();

            private Cleanup queue = () => _queueListener.Purge(_queueUrl);

            private static Message _message;
            private static SqsMessageProducer _messageProducer;
            private static TestAWSQueueListener _queueListener;
            private static string _listenedMessage;
            private static string _queueUrl = "https://sqs.eu-west-1.amazonaws.com/027649620536/brighter-test-queue";
        }
    }

    [Subject("Messaging Gateway")]
    [Tags("Requires", new[] { "AWSSDK" })]
    public class when_reading_a_message_via_the_messaging_gateway
    {
        Establish context = () =>
        {
            var logger = LogProvider.For<RmqMessageConsumer>();

            var messageHeader = new MessageHeader(Guid.NewGuid(), "test2", MessageType.MT_COMMAND);

            messageHeader.UpdateHandledCount();
            sentMessage = new Message(header: messageHeader, body: new MessageBody("test content"));

            sender = new SqsMessageProducer(queueUrl, logger);
            receiver = new SqsMessageConsumer(queueUrl, logger);
            testQueueListener = new TestAWSQueueListener(queueUrl);
        };

        Because of = () => sender.Send(sentMessage).ContinueWith(
            x =>
            {
                receivedMessage = receiver.Receive(2000);
                receiver.Acknowledge(receivedMessage);        
            }).Wait();

        It should_send_a_message_via_sqs_with_the_matching_body = () => receivedMessage.Body.ShouldEqual(sentMessage.Body);
        It should_send_a_message_via_sqs_with_the_matching_header_handled_count = () => receivedMessage.Header.HandledCount.ShouldEqual(sentMessage.Header.HandledCount);
        It should_send_a_message_via_sqs_with_the_matching_header_id = () => receivedMessage.Header.Id.ShouldEqual(sentMessage.Header.Id);
        It should_send_a_message_via_sqs_with_the_matching_header_message_type = () => receivedMessage.Header.MessageType.ShouldEqual(sentMessage.Header.MessageType);
        It should_send_a_message_via_sqs_with_the_matching_header_time_stamp = () => receivedMessage.Header.TimeStamp.ShouldEqual(sentMessage.Header.TimeStamp);
        It should_send_a_message_via_sqs_with_the_matching_header_topic = () => receivedMessage.Header.Topic.ShouldEqual(sentMessage.Header.Topic);
        It should_remove_the_message_from_the_queue = () => testQueueListener.Listen().ShouldBeNull();

        Cleanup the_queue = () => testQueueListener.Purge(queueUrl);

        private static TestAWSQueueListener testQueueListener;
        private static string queueUrl = "https://sqs.eu-west-1.amazonaws.com/027649620536/brighter-test-queue";
        private static IAmAMessageProducer sender;
        private static IAmAMessageConsumer receiver;
        private static Message sentMessage;
        private static Message receivedMessage;
    }

    public class when_rejecting_a_message_through_gateway_with_requeue
    {
        Establish context = () =>
        {
            var logger = LogProvider.For<RmqMessageConsumer>();

            var messageHeader = new MessageHeader(Guid.NewGuid(), "test2", MessageType.MT_COMMAND);

            messageHeader.UpdateHandledCount();
            message = new Message(header: messageHeader, body: new MessageBody("test content"));

            sender = new SqsMessageProducer(queueUrl, logger);
            receiver = new SqsMessageConsumer(queueUrl, logger);
            testQueueListener = new TestAWSQueueListener(queueUrl);


            var task = sender.Send(message);

            task.ContinueWith(x => { if (x.IsCompleted)_listenedMessage = receiver.Receive(1000);
                }).Wait();
        };

        Because i_reject_the_message = () => receiver.Reject(_listenedMessage, true);

        private It should_requeue_the_message = () =>
        {
            var message = receiver.Receive(1000);
            message.ShouldEqual(_listenedMessage);
        };

        Cleanup the_queue = () => testQueueListener.Purge(queueUrl);

        private static TestAWSQueueListener testQueueListener;
        private static string queueUrl = "https://sqs.eu-west-1.amazonaws.com/027649620536/brighter-test-queue";
        private static IAmAMessageProducer sender;
        private static IAmAMessageConsumer receiver;
        private static Message message;
        private static Message receivedMessage;
        private static Message _listenedMessage;
    }

    public class when_rejecting_a_message_through_gateway_without_requeue
    {
        Establish context = () =>
        {
            var logger = LogProvider.For<RmqMessageConsumer>();

            var messageHeader = new MessageHeader(Guid.NewGuid(), "test2", MessageType.MT_COMMAND);

            messageHeader.UpdateHandledCount();
            message = new Message(header: messageHeader, body: new MessageBody("test content"));

            sender = new SqsMessageProducer(queueUrl, logger);
            receiver = new SqsMessageConsumer(queueUrl, logger);
            testQueueListener = new TestAWSQueueListener(queueUrl);


            var task = sender.Send(message);

            task.ContinueWith(x => { if (x.IsCompleted)_listenedMessage = receiver.Receive(1000);
                }).Wait();
        };

        Because i_reject_the_message = () => receiver.Reject(_listenedMessage, false);

        private It should_not_requeue_the_message = () =>
        {
            testQueueListener.Listen().ShouldBeNull();
        };

        Cleanup the_queue = () => testQueueListener.Purge(queueUrl);

        private static TestAWSQueueListener testQueueListener;
        private static string queueUrl = "https://sqs.eu-west-1.amazonaws.com/027649620536/brighter-test-queue";
        private static IAmAMessageProducer sender;
        private static IAmAMessageConsumer receiver;
        private static Message message;
        private static Message receivedMessage;
        private static Message _listenedMessage;
    }

    public class when_purging_the_queue
    {
        private Establish context = () =>
        {
            var logger = LogProvider.For<RmqMessageConsumer>();

            var messageHeader = new MessageHeader(Guid.NewGuid(), "test2", MessageType.MT_COMMAND);

            messageHeader.UpdateHandledCount();
            sentMessage = new Message(header: messageHeader, body: new MessageBody("test content"));

            sender = new SqsMessageProducer(queueUrl, logger);
            receiver = new SqsMessageConsumer(queueUrl, logger);
            testQueueListener = new TestAWSQueueListener(queueUrl);
        };

        Because of = () => sender.Send(sentMessage).ContinueWith(
            x => receiver.Purge()).Wait();

        It should_clean_the_queue = () => testQueueListener.Listen().ShouldBeNull();

        Cleanup the_queue = () => testQueueListener.Purge(queueUrl);
        
        private static TestAWSQueueListener testQueueListener;
        private static IAmAMessageProducer sender;
        private static string queueUrl = "https://sqs.eu-west-1.amazonaws.com/027649620536/brighter-test-queue"; 
        private static IAmAMessageConsumer receiver;
        private static Message sentMessage;
    }

    public class when_requeueing_a_message
    {
        private Establish context = () =>
        {
            var logger = LogProvider.For<RmqMessageConsumer>();

            var messageHeader = new MessageHeader(Guid.NewGuid(), "test2", MessageType.MT_COMMAND);

            messageHeader.UpdateHandledCount();
            sentMessage = new Message(header: messageHeader, body: new MessageBody("test content"));

            sender = new SqsMessageProducer(queueUrl, logger);
            receiver = new SqsMessageConsumer(queueUrl, logger);
            testQueueListener = new TestAWSQueueListener(queueUrl);
        };

        Because of = () => sender.Send(sentMessage).ContinueWith(
            x =>
            {
                receivedMessage = receiver.Receive(2000);
                receiver.Requeue(receivedMessage);
            }).Wait();

        It should_delete_the_original_message_and_create_new_message = () => {
            var message = receiver.Receive(1000);
            message.Body.Value.ShouldEqual(receivedMessage.Body.Value);
            message.Header.Bag["ReceiptHandle"].ToString().ShouldNotEqual(receivedMessage.Header.Bag["ReceiptHandle"].ToString());
        };

        private static TestAWSQueueListener testQueueListener;
        private static IAmAMessageProducer sender;
        private static string queueUrl = "https://sqs.eu-west-1.amazonaws.com/027649620536/brighter-test-queue";
        private static IAmAMessageConsumer receiver;
        private static Message sentMessage;
        private static Message receivedMessage;
    }
}