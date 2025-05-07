// KafkaLikeMessagingQueue.cs

using System.Net.Sockets;
using System.Text;

namespace Popka.Messaging
{
    /// <summary>
    /// Produces messages and sends them to a topic in the broker via TCP.
    /// </summary>
    public class Producer
    {
        private readonly string _host;
        private readonly int _port;

        public Producer(string host = "127.0.0.1", int port = 7589)
        {
            _host = host;
            _port = port;
        }

        private async Task<string> SendRequestAsync(string request)
        {
            using var client = new TcpClient();
            await client.ConnectAsync(_host, _port);
            using var stream = client.GetStream();
            using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
            using var reader = new StreamReader(stream, Encoding.UTF8);

            await writer.WriteLineAsync(request);
            string response = await reader.ReadLineAsync();
            return response;
        }

        public async Task<(int PartitionId, long Offset)?> SendAsync(string topicName, string key, string value)
        {
            // Base64 encode value to handle special characters and newlines safely in one line.
            var valueBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(value ?? ""));
            string command = $"PRODUCE {topicName} {key ?? "__NULL__"} {valueBase64}";

            Console.WriteLine($"[Producer] Sending command: {command}");
            string response = await SendRequestAsync(command);
            Console.WriteLine($"[Producer] Received response: {response}");

            if (response != null && response.StartsWith("OK"))
            {
                var parts = response.Split(' ');
                return (int.Parse(parts[1]), long.Parse(parts[2]));
            }
            Console.WriteLine($"[Producer] Error: {response}");
            return null;
        }
        public async Task CreateTopicAsync(string topicName, int numPartitions)
        {
            string command = $"CREATE_TOPIC {topicName} {numPartitions}";
            Console.WriteLine($"[Producer/Admin] Sending command: {command}");
            string response = await SendRequestAsync(command);
            Console.WriteLine($"[Producer/Admin] Received response: {response}");
            if (response == null || !response.StartsWith("OK"))
            {
                throw new Exception($"Failed to create topic: {response}");
            }
        }
    }
}
