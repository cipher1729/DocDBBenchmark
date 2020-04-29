using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Configuration;
using System.Diagnostics;
using System.Net;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System.Globalization;
using Microsoft.Extensions.Logging;

namespace DocDBBenchmarkFunction
{
    /// <summary>
    /// This sample demonstrates how to achieve high performance writes using DocumentDB.
    /// </summary>
    public sealed class BenchmarkRunner
    {
        private static string DatabaseName = Environment.GetEnvironmentVariable("DatabaseName");
        private static string DataCollectionName = Environment.GetEnvironmentVariable("DataCollectionName");
        private static int CollectionThroughput = int.Parse(Environment.GetEnvironmentVariable("CollectionThroughput"));
        private static int DegreeOfParallelism = int.Parse(Environment.GetEnvironmentVariable("DegreeOfParallelism"));
        private static int NumberOfDocumentsToInsert = int.Parse(Environment.GetEnvironmentVariable("NumberOfDocumentsToInsert"));

        /// <summary>s

        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp,
            RequestTimeout = new TimeSpan(1, 0, 0),
            MaxConnectionLimit = 1000,
            RetryOptions = new RetryOptions
            {
                MaxRetryAttemptsOnThrottledRequests = 10,
                MaxRetryWaitTimeInSeconds = 60
            }
        };

        private const int MinThreadPoolSize = 100;

        private int pendingTaskCount;
        private long documentsInserted;
        private ConcurrentDictionary<int, double> requestUnitsConsumed = new ConcurrentDictionary<int, double>();
        private ConcurrentBag<string> seenExceptions = new ConcurrentBag<string>();
        public ConcurrentBag<string> exceptionDetails = new ConcurrentBag<string>();
        private DocumentClient client;

        /// <summary>
        /// Initializes a new instance of the <see cref="BenchmarkRunner"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        private BenchmarkRunner(DocumentClient client)
        {
            this.client = client;
        }

        
        /// <summary>
        /// Run samples for Order By queries.
        /// </summary>
        /// <returns>a Task object.</returns>
        private async Task<List<string>> RunAsync(int DegreeOfParallelism, int NumberOfDocumentsToInsert, ILogger logger, Microsoft.Azure.WebJobs.ExecutionContext context)
        {
            DocumentCollection dataCollection = GetCollectionIfExists(DatabaseName, DataCollectionName);
            int currentCollectionThroughput = 0;
            logger.LogInformation($"BenchmarkRunner :inside RunAsync method");

            if (bool.Parse(Environment.GetEnvironmentVariable("ShouldCleanupOnStart")) || dataCollection == null)
            {
                Database database = GetDatabaseIfExists(DatabaseName);
                if (database != null)
                {
                    await client.DeleteDatabaseAsync(database.SelfLink);
                }

                Console.WriteLine("Creating database {0}", DatabaseName);
                database = await client.CreateDatabaseAsync(new Database { Id = DatabaseName });

                Console.WriteLine("Creating collection {0} with {1} RU/s", DataCollectionName, CollectionThroughput);
                dataCollection = await this.CreatePartitionedCollectionAsync(DatabaseName, DataCollectionName);

                currentCollectionThroughput = CollectionThroughput;
            }
            else
            {
                OfferV2 offer = (OfferV2)client.CreateOfferQuery().Where(o => o.ResourceLink == dataCollection.SelfLink).AsEnumerable().FirstOrDefault();
                currentCollectionThroughput = offer.Content.OfferThroughput;

                Console.WriteLine("Found collection {0} with {1} RU/s", DataCollectionName, currentCollectionThroughput);
            }

            int taskCount;
            int degreeOfParallelism = DegreeOfParallelism;

            if (degreeOfParallelism == -1)
            {
                // set TaskCount = 10 for each 10k RUs, minimum 1, maximum 250
                taskCount = Math.Max(currentCollectionThroughput / 1000, 1);
                taskCount = Math.Min(taskCount, 250);
            }
            else
            {
                taskCount = degreeOfParallelism;
            }

            Console.WriteLine("Starting Inserts with {0} tasks", taskCount);
            logger.LogInformation($"BenchmarkRunner :Starting Inserts with {taskCount} tasks");

            //special way to read documents in azure functions 
            //from https://stackoverflow.com/questions/46537758/including-a-file-when-i-publish-my-azure-function-in-visual-studio
            string sampleDocument = File.ReadAllText(Path.Combine(context.FunctionAppDirectory, Environment.GetEnvironmentVariable("DocumentTemplateFile")));
            
            pendingTaskCount = taskCount;
            var tasks = new List<Task>();
            var logOutputStatesTask = this.LogOutputStats();
            tasks.Add(logOutputStatesTask);

            long numberOfDocumentsToInsert = NumberOfDocumentsToInsert / taskCount;
            
            for (var i = 0; i < taskCount; i++)
            {
                tasks.Add(this.InsertDocument(i, client, dataCollection, sampleDocument, numberOfDocumentsToInsert, logger));
            }

            await Task.WhenAll(tasks);

            if (bool.Parse(Environment.GetEnvironmentVariable("ShouldCleanupOnFinish")))
            {
                Console.WriteLine("Deleting Database {0}", DatabaseName);
                await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseName));
            }

            return await logOutputStatesTask;
        }

        private async Task InsertDocument(int taskId, DocumentClient client, DocumentCollection collection, string sampleJson, long numberOfDocumentsToInsert, ILogger logger)
        {
            requestUnitsConsumed[taskId] = 0;
            string partitionKeyProperty = collection.PartitionKey.Paths[0].Replace("/", "");
            Dictionary<string, object> newDictionary = JsonConvert.DeserializeObject<Dictionary<string, object>>(sampleJson);
        
            for (var i = 0; i < numberOfDocumentsToInsert; i++)
            {
                newDictionary["id"] = Guid.NewGuid().ToString();
                newDictionary[partitionKeyProperty] = Guid.NewGuid().ToString();

                try
                {
                    ResourceResponse<Document> response = await client.CreateDocumentAsync(
                            UriFactory.CreateDocumentCollectionUri(DatabaseName, DataCollectionName),
                            newDictionary,
                            new RequestOptions() { });

                    string partition = response.SessionToken.Split(':')[0];
                    requestUnitsConsumed[taskId] += response.RequestCharge;
                    Interlocked.Increment(ref this.documentsInserted);
                }

                catch (Exception e)
                {
                    if (e is DocumentClientException)
                    {
                        DocumentClientException de = (DocumentClientException)e;
                        if (de.StatusCode != HttpStatusCode.Forbidden)
                        {
                            if (!seenExceptions.Contains(de.Error.Code))
                            {
                                logger.LogError($"Exception occured {JsonConvert.SerializeObject(de)}");
                                seenExceptions.Add(de.Error.Code);
                                exceptionDetails.Add($"Exception occured {JsonConvert.SerializeObject(de)}");
                            }
                        }
                        else
                        {
                            Interlocked.Increment(ref this.documentsInserted);
                        }
                    }
                }
            }

            Interlocked.Decrement(ref this.pendingTaskCount);
        }

        private async Task<List<string>> LogOutputStats()
        {
            long lastCount = 0;
            double lastRequestUnits = 0;
            double lastSeconds = 0;
            double requestUnits = 0;
            double ruPerSecond = 0;
            double ruPerMonth = 0;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            while (this.pendingTaskCount > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                double seconds = watch.Elapsed.TotalSeconds;

                requestUnits = 0;
                foreach (int taskId in requestUnitsConsumed.Keys)
                {
                    requestUnits += requestUnitsConsumed[taskId];
                }

                long currentCount = this.documentsInserted;
                ruPerSecond = (requestUnits / seconds);
                ruPerMonth = ruPerSecond * 86400 * 30;

                Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                    currentCount,
                    Math.Round(this.documentsInserted / seconds),
                    Math.Round(ruPerSecond),
                    Math.Round(ruPerMonth / (1000 * 1000 * 1000)));

                lastCount = documentsInserted;
                lastSeconds = seconds;
                lastRequestUnits = requestUnits;
            }

            double totalSeconds = watch.Elapsed.TotalSeconds;
            ruPerSecond = (requestUnits / totalSeconds);
            ruPerMonth = ruPerSecond * 86400 * 30;

            var lines = new List<string>();
            Console.WriteLine();
            Console.WriteLine("Summary:");
            lines.Add("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            lines.Add("--------------------------------------------------------------------- ");

            Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                lastCount,
                Math.Round(this.documentsInserted / watch.Elapsed.TotalSeconds),
                Math.Round(ruPerSecond),
                Math.Round(ruPerMonth / (1000 * 1000 * 1000)));
            lines.Add($"Inserted {lastCount} docs @{Math.Round(ruPerSecond)} RU/s, Total Time taken {watch.Elapsed.TotalSeconds}s");

            Console.WriteLine("--------------------------------------------------------------------- ");
            return lines;
        }

        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
        private async Task<DocumentCollection> CreatePartitionedCollectionAsync(string databaseName, string collectionName)
        {
            DocumentCollection existingCollection = GetCollectionIfExists(databaseName, collectionName);

            DocumentCollection collection = new DocumentCollection();
            collection.Id = collectionName;
            collection.PartitionKey.Paths.Add(Environment.GetEnvironmentVariable("CollectionPartitionKey"));

            // Show user cost of running this test
            double estimatedCostPerMonth = 0.06 * CollectionThroughput;
            double estimatedCostPerHour = estimatedCostPerMonth / (24 * 30);
            Console.WriteLine("The collection will cost an estimated ${0} per hour (${1} per month)", Math.Round(estimatedCostPerHour, 2), Math.Round(estimatedCostPerMonth, 2));

            return await client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    collection,
                    new RequestOptions { OfferThroughput = CollectionThroughput });
        }

        /// <summary>
        /// Get the database if it exists, null if it doesn't
        /// </summary>
        /// <returns>The requested database</returns>
        private Database GetDatabaseIfExists(string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Get the collection if it exists, null if it doesn't
        /// </summary>
        /// <returns>The requested collection</returns>
        private DocumentCollection GetCollectionIfExists(string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(databaseName) == null)
            {
                return null;
            }

            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        public static class Harness
        {
           
            /// Main method for the sample.
            /// </summary>
            /// <param name="args">command line arguments.</param>
            public async static Task<string> InitializeAndRun(string databaseName, string dataCollectionName, string degreeOfParallelism, string numberOfDocumentsToInsert, string collectionThroughput, ILogger logger, Microsoft.Azure.WebJobs.ExecutionContext context)
            {

                if (databaseName != null)
                {
                    DatabaseName = databaseName;
                }
                if (dataCollectionName != null)
                {
                    DataCollectionName = dataCollectionName;
                }
                if (collectionThroughput != null)
                {
                    CollectionThroughput = Int32.Parse(collectionThroughput);
                }
                if (degreeOfParallelism != null)
                {
                    DegreeOfParallelism = Int32.Parse(degreeOfParallelism);
                }
                if (numberOfDocumentsToInsert != null)
                {
                    NumberOfDocumentsToInsert = Int32.Parse(numberOfDocumentsToInsert);
                }

                logger.LogInformation($"BenchmarkRunner running with parameters {DatabaseName} {DataCollectionName} {CollectionThroughput} {DegreeOfParallelism} {NumberOfDocumentsToInsert}");

                Console.WriteLine($"{DateTime.UtcNow}");

                ThreadPool.SetMinThreads(MinThreadPoolSize, MinThreadPoolSize);

                string endpoint = Environment.GetEnvironmentVariable("EndPointUrl");
                string authKey = Environment.GetEnvironmentVariable("AuthorizationKey");

                var lines = new List<string>();
                Console.WriteLine("Summary:");
                lines.Add("Summary:");

                Console.WriteLine("--------------------------------------------------------------------- ");
                lines.Add("--------------------------------------------------------------------- ");

                Console.WriteLine("Endpoint: {0}", endpoint);
                lines.Add($"Endpoint: {endpoint}");

                Console.WriteLine("Collection : {0}.{1} at {2} request units per second", DatabaseName, DataCollectionName, CollectionThroughput);
                lines.Add($"Collection : {DatabaseName}.{DataCollectionName} at {CollectionThroughput} request units per second");

                Console.WriteLine("Document Template*: {0}", Environment.GetEnvironmentVariable("DocumentTemplateFile"));

                Console.WriteLine("Degree of parallelism*: {0}", DegreeOfParallelism);
                lines.Add($"Degree of parallelism*: {DegreeOfParallelism}");

                Console.WriteLine("--------------------------------------------------------------------- ");
                Console.WriteLine();
                Console.WriteLine("DocumentDBBenchmark starting...");

                logger.LogInformation($"BenchmarkRunner :starting benchmark");
                try
                {
                    using (var client = new DocumentClient(
                        new Uri(endpoint),
                        authKey,
                        ConnectionPolicy))
                    {
                        var runner = new BenchmarkRunner(client);
                        lines.AddRange(await runner.RunAsync(DegreeOfParallelism, NumberOfDocumentsToInsert, logger, context));
                        if(runner.exceptionDetails.Count()!=0)
                        {
                            lines.Add(new string("Exception\n"));
                            lines.AddRange(runner.exceptionDetails);
                        }

                        Console.WriteLine("DocumentDBBenchmark completed successfully.");
                    }
                }

                catch (Exception e)
                {
                    // If the Exception is a DocumentClientException, the "StatusCode" value might help identity 
                    // the source of the problem. 
                    logger.LogInformation("Samples failed with exception:{0}", e);
                }

                //System.IO.File.WriteAllText(Directory.GetCurrentDirectory() + "\\Output.txt", String.Join("\n", lines));
                logger.LogInformation($"Benchmark runner result :  {String.Join("\n", lines)}");
                return String.Join("\n", lines);
            }
        }
    }

    
}
