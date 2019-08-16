namespace KafkaConsumerGroup
{
    using System;
    using System.Threading;
    using Confluent.Kafka;

    class Program
    {
        public static void Main(string[] args)
        {
            string topic = args[0];

            var conf = new ConsumerConfig
            {
                GroupId = "group-1",
                BootstrapServers = "localhost:9093,localhost:9092,localhost:9094,localhost:9095",
                
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                //c.Assign(new TopicPartition("topic-1", new Partition(0)));
                //c.AddBrokers("localhost:9093,localhost:9092,localhost:9094,localhost:9095");
                c.Subscribe(topic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            //var listofcr1 = c.Consume(TimeSpan.FromSeconds(5));
                            //c.Seek(new TopicPartitionOffset(TopicPartition = new TopicPartition("", 1), new Offset(20));

                            Console.WriteLine($"Message '{cr.Value}', " +
                                $"Topic '{cr.Topic}', " +
                                $"Partition '{cr.Partition}', " +
                                $"Offset: '{cr.TopicPartitionOffset}'");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}
