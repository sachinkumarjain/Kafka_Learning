using System;
using Confluent.Kafka;

namespace Kafka_Training
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9093,localhost:9092,localhost:9094,localhost:9095",
                MessageSendMaxRetries = 2,
                BatchNumMessages = 2,
                QueueBufferingMaxKbytes = 1000,
                Acks = Acks.Leader, 
                ReconnectBackoffMaxMs = 1000
            };
            //config.BootstrapServers.Insert(4, "localhost:9092");
            //config.BootstrapServers.Insert(2, "localhost:9094");
            //config.BootstrapServers.Insert(3, "localhost:9095");

            void handler(DeliveryReport<Null, string> r)
            {
                Console.WriteLine(!r.Error.IsError ?
                    $"Delivered message to {r.TopicPartitionOffset}" :
                    $"Delivery error: {r.Error.Reason}");
            }

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                var stringValue = "";
                for (var i = 0; i < 10; i++)
                {
                    stringValue += i.ToString();
                    //producer.ProduceAsync("topic-1", new Message<Null, string> { Value = stringValue });
                    producer.Produce("topic-4", new Message<Null, string> { Value = stringValue }, handler);
                }
                producer.Flush(timeout: TimeSpan.FromSeconds(10));
            }
        }
    }
}
