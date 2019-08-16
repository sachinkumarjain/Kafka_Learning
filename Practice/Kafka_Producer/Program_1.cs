using System;
using System.Text;
using Confluent.Kafka;

namespace Kafka_Training.Serialization
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
                QueueBufferingMaxKbytes = 10,
                Acks = Acks.Leader,
                Partitioner = Partitioner.Random
            };
            //config.BootstrapServers.Insert(4, "localhost:9092");
            //config.BootstrapServers.Insert(2, "localhost:9094");
            //config.BootstrapServers.Insert(3, "localhost:9095");

            void handler(DeliveryReport<string, SomeMessage> r)
            {
                var data = r.Value;
                Console.WriteLine(!r.Error.IsError ?

                    $"Delivered message to { r.TopicPartitionOffset}" :
                    $"Delivery error: {r.Error.Reason}");
            }

            using (var producer = new ProducerBuilder<string, SomeMessage>(config).SetValueSerializer(new SomeMessageSerializer()).Build())
            {
                var stringValue = "";
                for (var i = 0; i < 100; i++)
                {
                    stringValue = i.ToString();
                    //producer.ProduceAsync("topic-1", new Message<Null, string> { Value = stringValue });
                    producer.Produce("topic-4", new Message<string, SomeMessage>
                    {
                        Key = "key203",
                        Value = new SomeMessage
                        {
                            Id = stringValue,
                            Name = stringValue
                        },
                        Headers = new MessageMetadata { }
                    }, handler);
                }
                producer.Flush(timeout: TimeSpan.FromSeconds(10));
            }
        }
    }

    public class SomeMessage
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public override string ToString()
        {
            return $"{Id}: {Name}";
        }
    }

    public class SomeMessageSerializer : ISerializer<SomeMessage>
    {
        public byte[] Serialize(SomeMessage data, SerializationContext context)
        {
            return Encoding.UTF8.GetBytes(data.ToString());
        }
    }
}
