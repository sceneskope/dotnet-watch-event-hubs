using McMaster.Extensions.CommandLineUtils;
using Microsoft.Azure.EventHubs;
using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace watch_event_hubs
{
    [Command(Description = "Dump messages from event hubs")]
    class Program
    {
        public static int Main(string[] args) => CommandLineApplication.Execute<Program>(args);

        [Argument(0, Description = "The connection string for the event hub")]
        [Required]
        public string ConnectionString { get; }

        [Option(CommandOptionType.NoValue, Description = "Get events from the start")]
        public bool FromStart { get; }

        public string ConsumerGroup { get; } = "$default";

        private async Task<int> OnExecute()
        {
            using (var cts = new CancellationTokenSource())
            {
                Console.CancelKeyPress += (o, e) =>
                {
                    Console.WriteLine("Shutting down...");
                    cts.Cancel();
                };
                var ct = cts.Token;

                var tcs = new TaskCompletionSource<bool>();
                var client = EventHubClient.CreateFromConnectionString(ConnectionString);
                using (ct.Register(() => tcs.SetResult(true)))
                {
                    var info = await client.GetRuntimeInformationAsync();
                    Console.WriteLine($"Got {info.PartitionCount} partitions");
                    var tasks = new Task[info.PartitionCount + 1];
                    for (var i = 0; i < info.PartitionCount; i++)
                    {
                        tasks[i] = RunForPartition(info.PartitionIds[i], client, ct);
                    }
                    tasks[tasks.Length - 1] = ClosingTask(client, tcs.Task);
                    await Task.WhenAll(tasks);
                }
                return 0;
            }
        }

        private async Task ClosingTask(EventHubClient client, Task taskToWaitBeforeClosing)
        {
            await taskToWaitBeforeClosing;
            Console.WriteLine("Closing");
            await client.CloseAsync();
            Console.WriteLine("Closed");
        }

        private async Task RunForPartition(string partitionId, EventHubClient client, CancellationToken ct)
        {
            Console.WriteLine($"Running for partition {partitionId} {(FromStart ? "from start" : "")}");
            try
            {
                var receiver = client.CreateReceiver(ConsumerGroup, partitionId, FromStart ? EventPosition.FromStart() : EventPosition.FromEnd());
                while (!ct.IsCancellationRequested)
                {
                    var messages = await receiver.ReceiveAsync(100);
                    foreach (var message in messages ?? Enumerable.Empty<EventData>())
                    {
                        var json = Encoding.UTF8.GetString(message.Body);
                        Console.WriteLine($"{message.SystemProperties.EnqueuedTimeUtc}: {message.SystemProperties.Offset} ({message.SystemProperties.SequenceNumber})\n{json}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing partition {partitionId}: {ex.Message}");
            }
        }
    }
}
