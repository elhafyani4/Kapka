namespace Popka.Messaging
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Kafka-like Messaging Queue Demo (TCP Server)...");

            // 1. Initialize the Broker (core logic)
            var broker = new KafkaesqueBroker();

            // 2. Start the TCP Server
            var server = new KafkaServer(broker);
            var serverTask = server.StartAsync(); // Run server in the background

            Console.WriteLine("Server starting... waiting a moment for it to initialize.");
            await Task.Delay(1000); // Give server a moment to start listening

            try
            {
                // --- Client Operations ---
                string topicName = "orders_tcp";
                int numPartitions = 2;

                // 3. Initialize Producer (TCP client)
                var producer = new Producer(); // Connects to 127.0.0.1:7589 by default

                // 3a. Client creates the topic
                Console.WriteLine($"\n--- Client creating topic '{topicName}' ---");
                await producer.CreateTopicAsync(topicName, numPartitions);


                // 4. Initialize Consumers (TCP clients)
                var consumer1 = new Consumer("Consumer-1-TCP");
                var consumer2 = new Consumer("Consumer-2-TCP");

                // 5. Consumers subscribe to the topic (fetches partition info from server)
                Console.WriteLine("\n--- Consumer-1 Subscribing ---");
                await consumer1.SubscribeAsync(topicName);
                Console.WriteLine("\n--- Consumer-2 Subscribing ---");
                await consumer2.SubscribeAsync(topicName);


                // 6. Producer sends some messages
                Console.WriteLine("\n--- Producing Messages ---");
                await producer.SendAsync(topicName, "user777", "OrderPlaced: SmartWatch");
                await Task.Delay(20);
                await producer.SendAsync(topicName, "user888", "OrderPlaced: Tablet");
                await Task.Delay(20);
                await producer.SendAsync(topicName, null, "SystemUpdate: Server v1.1");
                await Task.Delay(20);
                await producer.SendAsync(topicName, "user777", "OrderUpdated: SmartWatch Shipped");

                // 7. Consumers poll for messages
                Console.WriteLine("\n--- Consumer-1 Polling (Max 5 per partition) ---");
                var messagesC1 = await consumer1.PollAsync(maxMessagesPerPartition: 5);
                Console.WriteLine($"Consumer-1 polled {messagesC1.Count} messages in total this round.");

                Console.WriteLine("\n--- Consumer-2 Polling (Max 5 per partition, should get same initial messages) ---");
                var messagesC2 = await consumer2.PollAsync(maxMessagesPerPartition: 5);
                Console.WriteLine($"Consumer-2 polled {messagesC2.Count} messages in total this round.");

                // Simulate more messages and polling
                Console.WriteLine("\n--- Producing More Messages ---");
                await producer.SendAsync(topicName, "user888", "OrderUpdated: Tablet Delivered");
                await producer.SendAsync(topicName, "user777", "OrderFeedback: SmartWatch Awesome!");

                Console.WriteLine("\n--- Consumer-1 Polling Again ---");
                messagesC1 = await consumer1.PollAsync(maxMessagesPerPartition: 5);
                Console.WriteLine($"Consumer-1 polled {messagesC1.Count} additional messages this round.");

                Console.WriteLine("\n--- Consumer-2 Polling Again ---");
                messagesC2 = await consumer2.PollAsync(maxMessagesPerPartition: 5);
                Console.WriteLine($"Consumer-2 polled {messagesC2.Count} additional messages this round.");

                // Demonstrate Seek functionality for Consumer-1
                // Assuming some messages went to partition 0.
                Console.WriteLine("\n--- Consumer-1 Seeking Partition 0 to Offset 1 ---");
                await consumer1.SeekAsync(partitionId: 0, offset: 1); // Seek to the second message (offset 1) of partition 0
                messagesC1 = await consumer1.PollAsync(maxMessagesPerPartition: 5);
                Console.WriteLine($"Consumer-1 polled {messagesC1.Count} messages after seeking partition 0.");


            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred during client operations: {ex}");
            }
            finally
            {
                Console.WriteLine("\nClient operations finished. Press any key to stop the server and exit.");
                Console.ReadKey();
                server.Stop();
                await serverTask; // Wait for server to shut down
            }
        }
    }
}
