using System;

namespace paramore.brighter.commandprocessor.messaginggateway.awssqs
{
    internal static class UnixTimestamp
    {
        private static readonly DateTime s_unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static DateTime DateTimeFromUnixTimestampSeconds(long seconds)
        {
            return s_unixEpoch.AddSeconds(seconds);
        }

        public static long GetCurrentUnixTimestampSeconds()
        {
            return (long)(DateTime.UtcNow - s_unixEpoch).TotalSeconds;
        }

        public static long GetUnixTimestampSeconds(DateTime dateTime)
        {
            return (long)(dateTime - s_unixEpoch).TotalSeconds;
        }
    }
}