// KafkaLikeMessagingQueue.cs

using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;

namespace Popka.Messaging
{
    /// <summary>
    /// Consumes messages from a topic via TCP.
    /// </summary>
    public class Consumer
    {
        private readonly string _host;
        private readonly int _port;
        private readonly string _consumerId;

        private string _subscribedTopicName;
        private List<int> _assignedPartitionIds = new List<int>();
        // Key: PartitionId, Value: Next Offset to read
        private readonly ConcurrentDictionary<int, long> _partitionOffsets = new ConcurrentDictionary<int, long>();

        public Consumer(string consumerId, string host = "127.0.0.1", int port = 7589)
        {
            _consumerId = consumerId;
            _host = host;
            _port = port;
        }

        private async Task<string[]> SendRequestAndReadMultiLineResponseAsync(string request, TcpClient client, StreamReader reader, StreamWriter writer)
        {
            await writer.WriteLineAsync(request);
            var lines = new List<string>();
            string line;
            // First line is the primary status/header
            line = await reader.ReadLineAsync();
            if (line == null) return new string[] { "ERROR Client disconnected prematurely" };
            lines.Add(line);

            if (line.StartsWith("MESSAGES")) // Expecting more lines for messages
            {
                var headerParts = line.Split(' '); // MESSAGES <nextOffset>
                // Second line is message count
                var countLine = await reader.ReadLineAsync();
                if (countLine == null) { lines.Add("ERROR Missing message count"); return lines.ToArray(); }
                lines.Add(countLine);

                if (int.TryParse(countLine, out int messageCount))
                {
                    for (int i = 0; i < messageCount; i++)
                    {
                        var msgLine = await reader.ReadLineAsync();
                        if (msgLine == null) { lines.Add($"ERROR Missing message data line {i + 1}"); break; }
                        lines.Add(msgLine);
                    }
                }
                else
                {
                    lines.Add("ERROR Invalid message count format");
                }
            }
            return lines.ToArray();
        }


        public async Task SubscribeAsync(string topicName)
        {
            _subscribedTopicName = topicName;
            string command = $"GET_PARTITIONS {topicName}";

            Console.WriteLine($"[Consumer {_consumerId}] Sending command: {command}");

            using var client = new TcpClient();
            await client.ConnectAsync(_host, _port);
            using var stream = client.GetStream();
            using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
            using var reader = new StreamReader(stream, Encoding.UTF8);

            await writer.WriteLineAsync(command);
            string response = await reader.ReadLineAsync();
            Console.WriteLine($"[Consumer {_consumerId}] Received response for GET_PARTITIONS: {response}");

            if (response != null && response.StartsWith("PARTITIONS"))
            {
                _assignedPartitionIds = response.Split(' ').Skip(1).Select(int.Parse).ToList();
                foreach (var pId in _assignedPartitionIds)
                {
                    _partitionOffsets.TryAdd(pId, 0L); // Start from offset 0 for each partition
                }
                Console.WriteLine($"[Consumer {_consumerId}] Subscribed to topic '{topicName}' and found partitions: {string.Join(", ", _assignedPartitionIds)}");
            }
            else
            {
                Console.WriteLine($"[Consumer {_consumerId}] Error subscribing: {response}. Topic might not exist or no partitions found.");
                // Optionally, try to create the topic if it's a common pattern, or throw.
                // For this demo, we assume topic is pre-created or producer creates it.
                // If topic is auto-created by server on produce, consumer might need to retry subscription.
                _assignedPartitionIds.Clear(); // Ensure no partitions if subscription failed.
            }
        }

        public async Task<List<Message>> PollAsync(int maxMessagesPerPartition)
        {
            var receivedMessages = new List<Message>();
            if (string.IsNullOrEmpty(_subscribedTopicName) || !_assignedPartitionIds.Any())
            {
                Console.WriteLine($"[Consumer {_consumerId}] Not subscribed to any topic or no partitions assigned.");
                return receivedMessages;
            }

            using var client = new TcpClient(); // Consider keeping client open for session or using a pool
            await client.ConnectAsync(_host, _port);
            using var stream = client.GetStream();
            using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
            using var reader = new StreamReader(stream, Encoding.UTF8);

            foreach (var partitionId in _assignedPartitionIds)
            {
                if (receivedMessages.Count >= maxMessagesPerPartition * _assignedPartitionIds.Count) break; // Overall limit

                long currentOffset = _partitionOffsets.GetOrAdd(partitionId, 0L);
                string command = $"CONSUME {_subscribedTopicName} {partitionId} {currentOffset} {maxMessagesPerPartition}";

                Console.WriteLine($"[Consumer {_consumerId}] Sending command: {command}");
                string[] responseLines = await SendRequestAndReadMultiLineResponseAsync(command, client, reader, writer);
                Console.WriteLine($"[Consumer {_consumerId}] Received response for partition {partitionId}: {string.Join("\\n", responseLines.Take(2))}{(responseLines.Length > 2 ? "..." : "")}");


                if (responseLines.Length > 0 && responseLines[0].StartsWith("MESSAGES"))
                {
                    var headerParts = responseLines[0].Split(' '); // MESSAGES <nextOffset>
                    long nextOffset = long.Parse(headerParts[1]);

                    if (responseLines.Length > 1 && int.TryParse(responseLines[1], out int messageCount) && messageCount > 0)
                    {
                        for (int i = 0; i < messageCount; i++)
                        {
                            if (responseLines.Length > 2 + i)
                            {
                                Message msg = Message.Deserialize(responseLines[2 + i]);
                                receivedMessages.Add(msg);
                                Console.WriteLine($"[Consumer {_consumerId}] Received message '{msg.Value}' (Key: {msg.Key ?? "N/A"}) from {_subscribedTopicName} - Partition {partitionId}");
                            }
                        }
                    }
                    _partitionOffsets[partitionId] = nextOffset; // "Commit" by updating to the next offset server told us
                }
                else if (responseLines.Length > 0)
                {
                    Console.WriteLine($"[Consumer {_consumerId}] Error polling partition {partitionId}: {responseLines[0]}");
                }
            }
            return receivedMessages;
        }

        public async Task SeekAsync(int partitionId, long offset) // Basic client-side seek
        {
            if (!_assignedPartitionIds.Contains(partitionId))
            {
                Console.WriteLine($"[Consumer {_consumerId}] Cannot seek. Not subscribed to partition {partitionId} of topic '{_subscribedTopicName}'.");
                return;
            }

            // For a more robust seek, you might want to validate against server's high offset
            // string command = $"GET_HIGH_OFFSET {_subscribedTopicName} {partitionId}";
            // ... send command, get highOffset ...
            // if (offset < 0 || offset > highOffset) { /* handle error */ }

            _partitionOffsets[partitionId] = offset;
            Console.WriteLine($"[Consumer {_consumerId}] Seeked partition {partitionId} to offset {offset}. Next poll will use this offset.");
        }
    }
}
