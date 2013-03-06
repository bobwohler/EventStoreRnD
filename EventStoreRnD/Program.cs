using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using EventStore.ClientAPI;

namespace marketdata
{
    class Program
    {
        static void Main(string[] args)
        {
            var ip = "192.168.3.100";
            int threads = 3;
            int toWrite = 1000;
            int numberOfInstruments = 1000;
            int toSleep = 0;
            ThreadPool.SetMinThreads(threads, threads);
            var instruments = PopulateInstruments(numberOfInstruments);
            var waitEvents = new List<AutoResetEvent>(Enumerable.Range(0, threads).Select(x => new AutoResetEvent(false)));
            using (var connection = EventStoreConnection.Create(ConnectionSettings.Create()))
            {
                connection.Connect(new IPEndPoint(IPAddress.Parse(ip), 1113));
                Enumerable.Range(0, threads).ForEach(
                    x =>
                    ThreadPool.QueueUserWorkItem(
                        c => WritePrices("ExchangeFeed" + x, toWrite, connection, instruments, toSleep, waitEvents[x])));
                waitEvents.ForEach(x => x.WaitOne());
                Console.WriteLine("done.");
                Console.ReadLine();
            }
        }

        private static int total;
        private static void WritePrices(string name, int toWrite, EventStoreConnection connection, Dictionary<string, decimal> instruments, int toSleep, AutoResetEvent waitEvent)
        {
            var keys = instruments.Keys.ToArray();
            var r = new Random();
            Enumerable.Range(0, toWrite).ForEach(x =>
            {
                var key = keys[r.Next(0, keys.Length)];
                var price = instruments[key];
                var newprice = price + (((decimal)r.NextDouble() - 0.5m) / 10.0m);
                connection.AppendToStream("ticks-" + key,
                                          ExpectedVersion.Any,
                                          new IEvent[]
                                                                                       {
                                                                                           new Event
                                                                                               {
                                                                                                   EventId = Guid.NewGuid(),
                                                                                                   Type = "TradeOccurred",
                                                                                                   IsJson = true,
                                                                                                   Data = GetEventData(key, newprice, r.Next(0,20), r.Next(0,20)),
                                                                                                   Metadata = GetEventMetaData(name)
                                                                                               }
                                                                                       }
                    );
                if (toSleep > 0)
                    Thread.Sleep(toSleep);
                var t = Interlocked.Increment(ref total);
                if (t % 100 == 0) Console.Write(".");
            });
            waitEvent.Set();
        }

        private static byte[] GetEventMetaData(string name)
        {
            var metaData = "{'feed' : '" + name + "'}";
            return Encoding.UTF8.GetBytes(metaData);
        }

        private static byte[] GetEventData(string key, decimal newprice, int buyer, int seller)
        {
            var side = (int)(new Random().Next(0, 100) / 50);
            var active = side == 0 ? "seller" : "buyer";
            var eventData = "{'instrument' : '" + key + "', 'newPrice' : '" +
                newprice.ToString("0.00") + "', 'timestamp' : " + GetMsSinceEpoch() + "," +
                "'buyer' : '" + buyer + "'," +
                "'seller' : '" + seller + "'," +
                "'activeside' : '" + active + "'"
                + "}";
            return Encoding.UTF8.GetBytes(eventData);
        }

        private static long GetMsSinceEpoch()
        {
            return (long)DateTime.Now.Subtract(new DateTime(1970, 1, 1)).TotalMilliseconds;
        }

        private static Dictionary<string, decimal> PopulateInstruments(int numberOfInstruments)
        {
            var ret = new Dictionary<string, decimal>();
            var r = new Random();
            var instrument = new[] { 'A', 'A', 'A' };
            Enumerable.Range(0, numberOfInstruments).ForEach(x =>
            {
                var copy = instrument.ToArray();
                copy[2] = (char)(copy[2] + x % 26);
                copy[1] = (char)(copy[1] + (x / 26) % 26);
                copy[0] = (char)(copy[0] + x / (26 * 26));
                ret.Add(new string(copy), r.Next(0, 100));
            });
            return ret;
        }

    }

    class Event : 
    {
        public Guid EventId { get; set; }
        public string Type { get; set; }
        public bool IsJson { get; set; }
        public byte[] Data { get; set; }
        public byte[] Metadata { get; set; }
    }

    static class IEnumerableExtensions
    {
        public static void ForEach<T>(this IEnumerable<T> enumerable, Action<T> action)
        {
            foreach (var e in enumerable) action(e);
        }
    }
}
