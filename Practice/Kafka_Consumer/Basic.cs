namespace KafkaConsumerBasic
{
    using System;
    using System.Threading;
    using Confluent.Kafka;

    class Program
    {
        public static void Main(string[] args)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "group-1",
                BootstrapServers = "localhost:9093,localhost:9092,localhost:9094,localhost:9095",

                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe("topic-4");

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
                            Console.WriteLine($"Message '{cr.Value}' at Partition Offset: '{cr.TopicPartitionOffset}'.");
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
