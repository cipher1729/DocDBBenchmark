# azure-documentdb-benchmark-core
#### Forked from https://github.com/Azure/azure-cosmos-dotnet-v2/tree/master/samples/documentdb-benchmark
#### .NET Core Azure COSMOS DB load tester with support for Azure functions

This code has been refactored from above. 

1. It has been converted to .Net core 3.1 to allow it to run on an Azure Function. 
2. Since Azure functions v2.0 run time does not support configuration through App.Config, all variables are accepted through either environment variable or console
3. Add logging . The http call to the function returns the logs that would have been generally streamed to console.
4. Log message texts have been modified.


To debug and run this locally:

1. Set environment variables 
        "ShouldCleanupOnFinish": "true",
        "ShouldCleanupOnStart": "false",
        "EndPointUrl": "<insert COSMOS DB account URI>",
        "CollectionPartitionKey": "/partitionKey",
        "DocumentTemplateFile": "Player.json",
        "AuthorizationKey": "<Primary Key for the Cosmos DB account>"

2. Pass in DatabaseName, DataCollectionName, NumberOfDocumentsToInsert, DegreeOfParallelism, , CollectionThroughput in the URL. (They can also be set through environment variables)

 [GET,POST] http://localhost:7071/api/DocDBBenchmark?DatabaseName=MeasuresAutoPilotDB&DataCollectionName=CslMetadataDocument&CollectionThroughput=400&DegreeOfParallelism=100&NumberOfDocumentsToInsert=900
 
 


