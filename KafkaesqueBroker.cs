// KafkaLikeMessagingQueue.cs

using System.Collections.Concurrent;

namespace Popka.Messaging
{
    /// <summary>
    /// The central broker that manages topics. (Largely unchanged)
    /// </summary>
    public class KafkaesqueBroker
    {
        private readonly ConcurrentDictionary<string, Topic> _topics = new ConcurrentDictionary<string, Topic>();
        private static readonly int DefaultPartitions = 3; // Default partitions if auto-created

        public Topic CreateOrGetTopic(string topicName, int numberOfPartitions = -1) // -1 indicates use default
        {
            if (numberOfPartitions == -1) numberOfPartitions = DefaultPartitions;
            return _topics.GetOrAdd(topicName, name =>
            {
                Console.WriteLine($"[Broker] Creating topic '{name}' with {numberOfPartitions} partitions.");
                return new Topic(name, numberOfPartitions);
            });
        }

        public Topic GetTopic(string topicName)
        {
            _topics.TryGetValue(topicName, out var topic);
            return topic;
        }
    }
}
