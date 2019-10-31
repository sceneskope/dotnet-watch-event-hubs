using Azure.Messaging.EventHubs;
using McMaster.Extensions.CommandLineUtils;
using System;
using System.ComponentModel.DataAnnotations;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace watch_event_hubs
{
    [Command(Description = "Dump messages from event hubs")]
    internal class Program
    {
        public static int Main(string[] args) => CommandLineApplication.Execute<Program>(args);

        [Argument(0, Description = "The connection string for the event hub")]
        [Required]
        public string ConnectionString { get; } = default!;

        [Option(CommandOptionType.NoValue, Description = "Get events from the start")]
        public bool FromStart { get; }

        public string ConsumerGroup { get; } = "$default";

        public async Task<int> OnExecute()
        {
            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (o, e) =>
            {
                Console.WriteLine("Shutting down...");
                cts.Cancel();
            };
            var ct = cts.Token;

            var tcs = new TaskCompletionSource<bool>();
            await using var client = new EventHubClient(ConnectionString);
            var partitions = await client.GetPartitionIdsAsync(ct);
            Console.WriteLine($"Got {partitions.Length} partitions");
            var tasks = new Task[partitions.Length];
            for (var i = 0; i < partitions.Length; i++)
            {
                tasks[i] = RunForPartition(partitions[i], client, ct);
            }
            await Task.WhenAll(tasks);
            return 0;
        }

        private async Task RunForPartition(string partitionId, EventHubClient client, CancellationToken ct)
        {
            Console.WriteLine($"Running for partition {partitionId} {(FromStart ? "from start" : "")}");
            try
            {
                await using var consumer = client.CreateConsumer(ConsumerGroup, partitionId, FromStart ? EventPosition.Earliest : EventPosition.Latest);
                await foreach (var message in consumer.SubscribeToEvents(ct))
                {
                    var json = Encoding.UTF8.GetString(message.Body.Span);
                    Console.WriteLine($"{message.EnqueuedTime}: {message.Offset} ({message.SequenceNumber})\n{json}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing partition {partitionId}: {ex.Message}");
            }
        }
    }
}
