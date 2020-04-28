using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Web;

namespace DocDBBenchmarkFunction
{
    public static class DocDBBenchmark
    {
        [FunctionName("DocDBBenchmark")]
        public async static Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log,
            ExecutionContext context)
        {
            var results = await BenchmarkRunner.Harness.InitializeAndRun(req.Query["DatabaseName"], req.Query["DataCollectionName"],  req.Query["DegreeOfParallelism"], req.Query["NumberOfDocumentsToInsert"], req.Query["CollectionThroughput"], log, context);
            return new OkObjectResult(results);
        }
    }
}
