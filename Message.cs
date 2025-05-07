// KafkaLikeMessagingQueue.cs

using System.Globalization;

namespace Popka.Messaging
{
    // Represents a message in the queue
    public record Message(string Key, string Value, DateTime Timestamp)
    {
        // Serialize to string for network transmission
        public string Serialize()
        {
            // Using "o" for round-trip date/time format
            return $"{Key?.Replace("|", "\\|") ?? "__NULL__"}|{Value?.Replace("|", "\\|")}|{Timestamp:o}";
        }

        // Deserialize from string
        public static Message Deserialize(string data)
        {
            var parts = data.Split(new[] { '|' }, 3);
            if (parts.Length != 3)
                throw new FormatException("Invalid message format for deserialization.");

            string key = parts[0] == "__NULL__" ? null : parts[0].Replace("\\|", "|");
            string value = parts[1].Replace("\\|", "|");
            DateTime timestamp = DateTime.Parse(parts[2], CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            return new Message(key, value, timestamp);
        }
    }
}
