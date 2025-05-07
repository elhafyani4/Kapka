// KafkaLikeMessagingQueue.cs

using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Popka.Messaging
{
    /// <summary>
    /// TCP Server to handle client requests for the Kafka-like queue.
    /// </summary>
    public class KafkaServer
    {
        private readonly TcpListener _listener;
        private readonly KafkaesqueBroker _broker;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private const int Port = 7589;

        public KafkaServer(KafkaesqueBroker broker)
        {
            _broker = broker;
            _listener = new TcpListener(IPAddress.Any, Port);
        }

        public async Task StartAsync()
        {
            _listener.Start();
            Console.WriteLine($"[Server] Listening on port {Port}...");

            try
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    TcpClient client = await _listener.AcceptTcpClientAsync(_cts.Token);
                    Console.WriteLine($"[Server] Client connected: {client.Client.RemoteEndPoint}");
                    _ = HandleClientAsync(client, _cts.Token); // Fire and forget client handler
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("[Server] Listener cancelled.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Server] Listener error: {ex.Message}");
            }
            finally
            {
                _listener.Stop();
                Console.WriteLine("[Server] Stopped.");
            }
        }

        public void Stop()
        {
            _cts.Cancel();
        }

        private async Task HandleClientAsync(TcpClient client, CancellationToken token)
        {
            string clientEndPointString = "unknown_client"; // Default if endpoint can't be read

            if (client == null)
            {
                Console.WriteLine("[Server] HandleClientAsync received a null client. Aborting.");
                return; // Cannot proceed if client is null
            }

            try
            {
                // Attempt to get the client's endpoint address for logging as early as possible.
                if (client.Client != null && client.Client.RemoteEndPoint != null)
                {
                    clientEndPointString = client.Client.RemoteEndPoint.ToString();
                }
                else
                {
                    // This state (client not null, but client.Client or RemoteEndPoint is null)
                    // is unexpected for a client returned by AcceptTcpClientAsync.
                    Console.WriteLine($"[Server] Warning: Client object is present but its underlying socket or RemoteEndPoint is null. Client.Connected: {client.Connected}");
                }
            }
            catch (ObjectDisposedException)
            {
                // This might happen if the client was disposed very quickly after connection,
                // or if client.Client.RemoteEndPoint access itself triggers it due to prior disposal.
                Console.WriteLine("[Server] Client was already disposed when trying to get RemoteEndPoint at handler start.");
                // clientEndPointString remains "unknown_client". The handler might exit quickly if client is unusable.
            }
            // All subsequent logging for this client should use clientEndPointString.

            try
            {
                // The 'using' statement ensures that 'client' and its resources are disposed
                // when this block is exited, either normally or due to an exception.
                using (client)
                using (var stream = client.GetStream())
                using (var reader = new StreamReader(stream, Encoding.UTF8))
                using (var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
                {
                    // Loop as long as cancellation is not requested and the client is considered connected.
                    // Accessing client.Connected can throw ObjectDisposedException if client is already disposed.
                    while (!token.IsCancellationRequested && client.Connected)
                    {
                        string commandLine = await reader.ReadLineAsync(token); // Can throw OperationCanceledException

                        if (commandLine == null)
                        {
                            // This typically means the client has closed the connection gracefully.
                            Console.WriteLine($"[Server] Client {clientEndPointString} disconnected gracefully (read null).");
                            break;
                        }

                        Console.WriteLine($"[Server] Received command from {clientEndPointString}: {commandLine}");
                        string response = await ProcessCommandAsync(commandLine);
                        await writer.WriteLineAsync(response.AsMemory(), token); // Can throw OperationCanceledException
                        Console.WriteLine($"[Server] Sent response to {clientEndPointString}: {response}");
                    }
                } // client, stream, reader, writer are disposed here.
            }
            catch (OperationCanceledException) // Catches cancellations from ReadLineAsync/WriteLineAsync if token is used.
            {
                Console.WriteLine($"[Server] Client handler operation cancelled for {clientEndPointString}.");
            }
            catch (IOException ex) // Handles network-related errors, like abrupt disconnections.
            {
                if (ex.InnerException is SocketException se)
                {
                    Console.WriteLine($"[Server] SocketException for client {clientEndPointString}: {se.Message} (SocketErrorCode: {se.SocketErrorCode})");
                }
                else
                {
                    Console.WriteLine($"[Server] IOException for client {clientEndPointString}: {ex.Message}");
                }
            }
            catch (ObjectDisposedException odx)
            {
                // This can catch ObjectDisposedException if, for example, client.Connected is accessed
                // after the client has been disposed by an earlier error/exit from the using block.
                Console.WriteLine($"[Server] ObjectDisposedException in client handler for {clientEndPointString}: {odx.Message}. Client was likely already closed.");
            }
            catch (Exception ex) // Catch-all for any other unexpected errors.
            {
                Console.WriteLine($"[Server] Unexpected error handling client {clientEndPointString}: {ex.GetType().Name} - {ex.Message}");
                // For detailed debugging, you might want to log the stack trace:
                // Console.WriteLine($"[Server] StackTrace for {clientEndPointString}: {ex.StackTrace}");
            }
            finally
            {
                // This block executes after the main 'try' (which includes the 'using' statements).
                // The 'client' TcpClient object itself is still in scope here, but it has been disposed by the 'using' statement.
                Console.WriteLine($"[Server] Client {clientEndPointString} processing finished and connection closed.");
            }
        }

        private async Task<string> ProcessCommandAsync(string commandLine)
        {
            // Simple command parsing. Robust parsing would use a library or more structure.
            var parts = commandLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0) return "ERROR Invalid command";

            string command = parts[0].ToUpperInvariant();

            try
            {
                switch (command)
                {
                    case "CREATE_TOPIC": // CREATE_TOPIC <topicName> <numPartitions>
                        if (parts.Length != 3) return "ERROR CREATE_TOPIC Usage: CREATE_TOPIC <topicName> <numPartitions>";
                        _broker.CreateOrGetTopic(parts[1], int.Parse(parts[2]));
                        return $"OK TOPIC_CREATED {parts[1]}";

                    case "PRODUCE": // PRODUCE <topicName> <key|__NULL__> <value_base64_encoded>
                        if (parts.Length != 4) return "ERROR PRODUCE Usage: PRODUCE <topicName> <key|__NULL__> <value_base64>";
                        var topicName = parts[1];
                        var key = parts[2] == "__NULL__" ? null : parts[2];
                        var value = Encoding.UTF8.GetString(Convert.FromBase64String(parts[3]));

                        var topic = _broker.GetTopic(topicName);
                        if (topic == null)
                        {
                            // Auto-create topic if it doesn't exist, as per previous logic for simplicity in demo
                            Console.WriteLine($"[Server] Topic '{topicName}' not found for PRODUCE. Auto-creating with default partitions.");
                            topic = _broker.CreateOrGetTopic(topicName);
                        }

                        var message = new Message(key, value, DateTime.UtcNow);
                        var (partitionId, offset) = topic.PublishMessage(message);
                        return $"OK {partitionId} {offset}";

                    case "GET_PARTITIONS": // GET_PARTITIONS <topicName>
                        if (parts.Length != 2) return "ERROR GET_PARTITIONS Usage: GET_PARTITIONS <topicName>";
                        var t = _broker.GetTopic(parts[1]);
                        if (t == null) return $"ERROR Topic {parts[1]} not found";
                        return $"PARTITIONS {string.Join(" ", t.PartitionsList.Select(p => p.Id))}";

                    case "CONSUME": // CONSUME <topicName> <partitionId> <offsetToReadFrom> <maxMessages>
                        if (parts.Length != 5) return "ERROR CONSUME Usage: CONSUME <topicName> <partitionId> <offset> <maxMessages>";
                        var consumeTopic = _broker.GetTopic(parts[1]);
                        if (consumeTopic == null) return $"ERROR Topic {parts[1]} not found";
                        var pId = int.Parse(parts[2]);
                        var consumePartition = consumeTopic.GetPartition(pId);
                        if (consumePartition == null) return $"ERROR Partition {pId} not found in topic {parts[1]}";

                        var (messages, nextOffset) = consumePartition.GetMessages(long.Parse(parts[3]), int.Parse(parts[4]));
                        var sb = new StringBuilder();
                        sb.AppendLine($"MESSAGES {nextOffset}");
                        sb.AppendLine(messages.Count.ToString());
                        foreach (var msg in messages)
                        {
                            sb.AppendLine(msg.Serialize());
                        }
                        return sb.ToString().TrimEnd();

                    case "GET_HIGH_OFFSET": // GET_HIGH_OFFSET <topicName> <partitionId>
                        if (parts.Length != 3) return "ERROR GET_HIGH_OFFSET Usage: GET_HIGH_OFFSET <topicName> <partitionId>";
                        var offsetTopic = _broker.GetTopic(parts[1]);
                        if (offsetTopic == null) return $"ERROR Topic {parts[1]} not found";
                        var offsetPId = int.Parse(parts[2]);
                        var offsetPartition = offsetTopic.GetPartition(offsetPId);
                        if (offsetPartition == null) return $"ERROR Partition {offsetPId} not found in topic {parts[1]}";
                        return $"HIGH_OFFSET {offsetPartition.CurrentOffset}";

                    default:
                        return "ERROR Unknown command";
                }
            }
            catch (FormatException fx)
            {
                return $"ERROR Invalid format in command: {fx.Message}";
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Server] Error processing command '{commandLine}': {ex}");
                return $"ERROR Internal server error: {ex.Message}";
            }
        }
    }
}
