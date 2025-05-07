// KafkaLikeMessagingQueue.cs

namespace Popka.Messaging
{
    /// <summary>
    /// Represents a partition within a topic. (Largely unchanged)
    /// Messages within a partition are ordered and have an offset.
    /// </summary>
    public class Partition
    {
        private readonly List<Message> _messages = new List<Message>();
        private readonly object _lock = new object();
        private long _currentOffset = 0;

        public int Id { get; }

        public Partition(int id)
        {
            Id = id;
        }

        public long AppendMessage(Message message)
        {
            lock (_lock)
            {
                _messages.Add(message);
                return _currentOffset++;
            }
        }

        public (List<Message> Messages, long NextOffset) GetMessages(long offset, int maxMessages)
        {
            lock (_lock)
            {
                if (offset < 0 || offset > _currentOffset) // Allow offset == _currentOffset for "empty" read
                {
                    return (new List<Message>(), _currentOffset);
                }
                if (offset == _currentOffset) // No new messages
                {
                    return (new List<Message>(), _currentOffset);
                }


                var messagesToReturn = new List<Message>();
                int count = 0;
                for (long i = offset; i < _currentOffset && count < maxMessages; i++, count++)
                {
                    messagesToReturn.Add(_messages[(int)i]);
                }
                return (messagesToReturn, offset + messagesToReturn.Count);
            }
        }

        public long CurrentOffset
        {
            get
            {
                lock (_lock)
                {
                    return _currentOffset;
                }
            }
        }
    }
}
