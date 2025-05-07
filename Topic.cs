// KafkaLikeMessagingQueue.cs

using System.Collections.Concurrent;

namespace Popka.Messaging
{
    /// <summary>
    /// Represents a topic, which is a collection of partitions. (Largely unchanged)
    /// </summary>
    public class Topic
    {
        public string Name { get; }
        private readonly List<Partition> _partitions;
        private readonly ConcurrentDictionary<int, Partition> _partitionMap = new ConcurrentDictionary<int, Partition>();
        private int _roundRobinCounter = 0;

        public IReadOnlyList<Partition> PartitionsList => _partitions.AsReadOnly(); // Renamed to avoid conflict

        public Topic(string name, int numberOfPartitions)
        {
            if (numberOfPartitions <= 0)
                throw new ArgumentOutOfRangeException(nameof(numberOfPartitions), "Number of partitions must be positive.");

            Name = name;
            _partitions = new List<Partition>(numberOfPartitions);
            for (int i = 0; i < numberOfPartitions; i++)
            {
                var partition = new Partition(i);
                _partitions.Add(partition);
                _partitionMap[i] = partition;
            }
        }

        public (int PartitionId, long Offset) PublishMessage(Message message)
        {
            Partition partition;
            if (!string.IsNullOrEmpty(message.Key))
            {
                int partitionIndex = Math.Abs(message.Key.GetHashCode()) % _partitions.Count;
                partition = _partitions[partitionIndex];
            }
            else
            {
                int partitionIndex = Interlocked.Increment(ref _roundRobinCounter) % _partitions.Count;
                partition = _partitions[partitionIndex];
            }

            long offset = partition.AppendMessage(message);
            return (partition.Id, offset);
        }

        public Partition GetPartition(int partitionId)
        {
            _partitionMap.TryGetValue(partitionId, out var partition);
            return partition;
        }
    }
}
