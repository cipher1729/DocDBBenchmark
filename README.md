# DocDBBenchmark
Forked from https://github.com/Azure/azure-cosmos-dotnet-v2/tree/master/samples/documentdb-benchmark

This is pretty much the same code as the above repo. However

1. It has been converted to .Net core 3.1 to allow it to run on an Azure Function. 
2. Since Azure functions run time does not support configuration through App.Config, all variables are accepted through either environment variable or console

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
 
 


