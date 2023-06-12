// This is a prototype tool that allows for extraction of data from a search index
// Since this tool is still under development, it should not be used for production usage

using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Models;
using log4net;
using log4net.Config;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AzureSearchBackupRestore
{
    class Program
    {
        //private static ILog logger = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly ILog logger = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly ILog summaryLog = LogManager.GetLogger("SummaryLogger");
        private static string SourceSearchServiceName;
        private static string SourceAdminKey;
        private static string SourceIndexName;
        private static string TargetSearchServiceName;
        private static string TargetAdminKey;
        private static string TargetIndexName;
        private static string BackupDirectory;

        private static SearchIndexClient SourceIndexClient;
        private static SearchClient SourceSearchClient;
        private static SearchIndexClient TargetIndexClient;
        private static SearchClient TargetSearchClient;
        //
        // Take JSON file and import this as-is to target index
        //private static Uri targetServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
        private static HttpClient targetHttpClient = new HttpClient();
        private static Boolean replicateIndexOnly = false;
        private static Boolean logSummary = false;

        private static int MaxBatchSize = 500;          // JSON files will contain this many documents / file and can be up to 1000
        private static int ParallelizedJobs = 2;       // Output content in parallel jobs
        //
        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure(new System.IO.FileInfo("log4net.config"));

            //Get source and target search service info and index names from appsettings.json file
            //Set up source and target search service clients
            ConfigurationSetup();
            if (replicateIndexOnly)
            {
                replicateIndex();
                return;
            }
            //Log Summary of Indexes
            if (logSummary)
            {
                //Process Summary
                processSummaryLogs();
                return;
            }
            //Backup the source index
            Console.WriteLine("\nSTART INDEX BACKUP");
            //Process configured Indexes
            List<string> indexesToProcess = SourceIndexName.Split(',').ToList();
            foreach (String indexName in indexesToProcess)
            {
                processDocuments(indexName);
            }


            //Recreate and import content to target index
            //Console.WriteLine("\nSTART INDEX RESTORE");
            //DeleteIndex();
            //CreateTargetIndex();
            //ImportFromJSON();
            //Console.WriteLine("\r\n  Waiting 10 seconds for target to index content...");
            //Console.WriteLine("  NOTE: For really large indexes it may take longer to index all content.\r\n");
            //Thread.Sleep(10000);

            // Validate all content is in target index
            //int sourceCount = GetCurrentDocCount(SourceSearchClient);
            //int targetCount = GetCurrentDocCount(TargetSearchClient);
            //Console.WriteLine("\nSAFEGUARD CHECK: Source and target index counts should match");
            //Console.WriteLine(" Source index contains {0} docs", sourceCount);
            //Console.WriteLine(" Target index contains {0} docs\r\n", targetCount);

            //Console.WriteLine("Press any key to continue...");
            //Console.ReadLine();
        }

        static void ConfigurationSetup()
        {

            IConfigurationBuilder builder = new ConfigurationBuilder().AddJsonFile("appsettings.json");
            IConfigurationRoot configuration = builder.Build();
            SourceSearchServiceName = configuration["SourceSearchServiceName"];
            SourceAdminKey = configuration["SourceAdminKey"];
            SourceIndexName = configuration["SourceIndexName"];
            TargetSearchServiceName = configuration["TargetSearchServiceName"];
            TargetAdminKey = configuration["TargetAdminKey"];
            TargetIndexName = configuration["TargetIndexName"];
            BackupDirectory = configuration["BackupDirectory"];
            replicateIndexOnly = bool.Parse(configuration["ReplicateIndexOnly"]);
            logSummary = bool.Parse(configuration["LogSummaryOnly"]);

            // Take JSON file and import this as-is to target index

        }
        static void replicateIndex()
        {
            String indexData = GetIndexSchema("All");
            JObject iData = JObject.Parse(indexData);
            JArray indexes = (JArray)iData.GetValue("value");
            summaryLog.Info(string.Format("** Replicating {0} Indexes **", indexes.Count));
            foreach (JObject item in indexes)
            {
                item.Remove("@odata.etag");
                string indexName = (string)item.GetValue("name");
                //Delete Index if exists.
                DeleteIndex(indexName);
                // Create Target Index
                CreateTargetIndex(item.ToString());
                summaryLog.Info(string.Format("Created Index {0}", indexName));
            }
            summaryLog.Info(string.Format("** Replicating Indexes Ended **"));
        }
        static void processSummaryLogs()
        {
            String indexData = GetIndexSchema("All");
            JObject iData = JObject.Parse(indexData);
            JArray indexes = (JArray)iData.GetValue("value");
            summaryLog.Info(string.Format("** Starting Summary Process Counts {0} **", indexes.Count));
            SourceIndexClient = new SearchIndexClient(new Uri("https://" + SourceSearchServiceName + ".search.windows.net"), new AzureKeyCredential(SourceAdminKey));
            TargetIndexClient = new SearchIndexClient(new Uri("https://" + TargetSearchServiceName + ".search.windows.net"), new AzureKeyCredential(TargetAdminKey));
            foreach (JObject item in indexes)
            {
                string indexName = (string)item.GetValue("name");
                //total documents From the Source index
                SourceSearchClient = SourceIndexClient.GetSearchClient(indexName);
                int indexDocCount = GetCurrentDocCount(SourceSearchClient);
                summaryLog.Info(string.Format("Source Index Count: {0},{1}", indexName, indexDocCount));
                // Target Index
                //total documents for the Target index
                TargetSearchClient = TargetIndexClient.GetSearchClient(indexName);
                int targetIndexDocCount = GetCurrentDocCount(TargetSearchClient);
                summaryLog.Info(string.Format("Target Index Count: {0},{1}", indexName, targetIndexDocCount));
            }
            summaryLog.Info(string.Format("** Ending Summary Process Counts **"));
        }   
        static void startReplicateProcess(string indexName)
        {

            // Backup the index schema to the specified backup directory
            //Console.WriteLine("\n  Backing up source index schema to {0}\r\n", BackupDirectory + "\\" + SourceIndexName + ".schema");

            String indexData = GetIndexSchema(indexName);

            //File.WriteAllText(BackupDirectory + "\\" + SourceIndexName + "-All" + ".schema", indexData);
            JObject iData = JObject.Parse(indexData);
            JArray indexes = (JArray)iData.GetValue("value");
            //var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = ParallelizedJobs };
            //List<JObject> items = indexes.ToObject<List<JObject>>();
            //Parallel.ForEach(indexes, parallelOptions, indexObject =>
            //{
            //    processDocuments((JObject)indexObject);
            //});
            int totalIndexes = indexes.Count;
            int currentIndex = 0;
            for (int i = 0; i < indexes.Count / ParallelizedJobs; i += ParallelizedJobs)
            {
                List<Task> tasks = new List<Task>();
                for (int job = 0; job < ParallelizedJobs; job++)
                {
                    if (currentIndex < totalIndexes)
                    {
                        // start the new task
                        JObject cindex = (JObject)indexes[currentIndex];
                        tasks.Add(Task.Factory.StartNew(() =>
                            processDocuments(cindex.ToString())
                        ));
                        currentIndex++;
                    }
                }
                Task.WaitAll(tasks.ToArray());
            }
            return;

        }

        private static int getDocID(int type)
        {
            string json = string.Empty;
            int topDocID = 0;
            try
            {
                SearchOptions options = new SearchOptions()
                {
                    SearchMode = SearchMode.All
                };
                if (type == 1)
                {
                    options.OrderBy.Add("id asc");
                }
                else
                {
                    options.OrderBy.Add("id desc");
                }

                SearchResults<SearchDocument> response = SourceSearchClient.Search<SearchDocument>("*", options);
                foreach (var doc in response.GetResults())
                {
                    JObject firstDoc = JObject.Parse(doc.Document.ToString());
                    topDocID = (int)firstDoc.GetValue("id");
                    break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }
            return topDocID;
            throw new NotImplementedException();
        }
        //Get Documents
        private static Task processDocuments(String indexName)
        {

            //total documents for the index
            SourceIndexClient = new SearchIndexClient(new Uri("https://" + SourceSearchServiceName + ".search.windows.net"), new AzureKeyCredential(SourceAdminKey));
            //
            SourceSearchClient = SourceIndexClient.GetSearchClient(indexName);
            int indexDocCount = GetCurrentDocCount(SourceSearchClient);
            //
            //TargetIndexClient = new SearchIndexClient(new Uri($"https://" + TargetSearchServiceName + ".search.windows.net"), new AzureKeyCredential(TargetAdminKey));
            //TargetSearchClient = TargetIndexClient.GetSearchClient(indexName);
            //
            // Uri ServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
            HttpClient HttpClient = new HttpClient();
            HttpClient.DefaultRequestHeaders.Add("api-key", TargetAdminKey);
            //targetHttpClient.DefaultRequestHeaders.Add("api-key", TargetAdminKey);
            Uri targetServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
            //
            Uri targetIndexUri = new Uri(targetServiceUri, "/indexes/" + indexName + "/docs/index");
            //
            logger.Info(string.Format("Processing Index: {0},{1},{2}", indexName, indexDocCount, DateTime.Now));
            summaryLog.Info(string.Format("Processing Index: {0},{1},{2}", indexName, indexDocCount, DateTime.Now));

            //get first id and last id
            int firstDocId = getDocID(1);
            int docIDLatest = firstDocId;
            int totalProcessed = 0;
            int numLoops = 0;
            int currentFetch = 0;
            while (true)
            {
                numLoops++;
                //logger.Info(String.Format("Documents Read,ID: {0},{1}", totalProcessed,docIDLatest));
                //Push to target
                string json = string.Empty;
                string jsonD = string.Empty;
                string filter = "id ge '" + docIDLatest + "'";
                int retryCount = 0;
                try
                {
                    SearchOptions options = new SearchOptions()
                    {
                        SearchMode = SearchMode.All,
                        Filter = filter,
                        Size = MaxBatchSize
                    };
                    options.OrderBy.Add("id asc");
                    SearchResults<SearchDocument> response = SourceSearchClient.Search<SearchDocument>("*", options);
                    //int docCount = response.GetResults().Count();

                    int docsFetched = 0;
                    foreach (var doc in response.GetResults())
                    {
                        // Console.WriteLine("\n {0}", doc);
                        docsFetched++;
                        jsonD = JsonSerializer.Serialize(doc.Document);
                        // Latest doc ID
                        docIDLatest = (int)JObject.Parse(jsonD).GetValue("id");
                        json += jsonD + ",";
                        json = json.Replace("\"Latitude\":", "\"type\": \"Point\", \"coordinates\": [");
                        json = json.Replace("\"Longitude\":", "");
                        json = json.Replace(",\"IsEmpty\":false,\"Z\":null,\"M\":null,\"CoordinateSystem\":{\"EpsgId\":4326,\"Id\":\"4326\",\"Name\":\"WGS84\"}", "]");
                        json += "\r\n";
                    }
                    //
                    json = json.Substring(0, json.Length - 3); // remove trailing comma
                                                               //    
                    json = "{\"value\": [" + json + "]}";
                    //
                    currentFetch = docsFetched;
                    //load into Target
                    HttpResponseMessage tresponse = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Post, targetIndexUri, json);
                    tresponse.EnsureSuccessStatusCode();
                    totalProcessed = totalProcessed + docsFetched;
                    retryCount = 0;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: {0}", ex.Message.ToString());
                    retryCount++;
                    if (retryCount > 3)
                    {
                        logger.Error(string.Format("Exception :{0}", ex));
                        break;
                    }
                }

                logger.Info(string.Format("Documents Created,total,ID: {0},{1},{2}", indexName, totalProcessed, docIDLatest));
                if (totalProcessed > indexDocCount + numLoops || currentFetch < 100)
                {
                    logger.Info(string.Format("**Completed Replicating**,total,ID: {0},{1},{2}", indexName, totalProcessed, docIDLatest));
                    summaryLog.Info(string.Format("**Completed Replicating** {0},{1},{2}", indexName, totalProcessed, DateTime.Now));
                    break;
                }
            }

            return Task.CompletedTask;
        }

        private static void writeToTarget(string json, string indexName)
        {
            Uri targetServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
            try
            {
                //string json = File.ReadAllText(fileName);
                Uri uri = new Uri(targetServiceUri, "/indexes/" + indexName + "/docs/index");
                HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(targetHttpClient, HttpMethod.Post, uri, json);
                response.EnsureSuccessStatusCode();

            }
            catch (Exception ex)
            {
                Console.WriteLine("  Error: {0}", ex.Message.ToString());
            }
        }
        static void WriteIndexDocuments(int CurrentDocCount)
        {
            // Write document files in batches (per MaxBatchSize) in parallel
            string IDFieldName = GetIDFieldName();
            int FileCounter = 0;
            for (int batch = 0; batch <= (CurrentDocCount / MaxBatchSize); batch += ParallelizedJobs)
            {

                List<Task> tasks = new List<Task>();
                for (int job = 0; job < ParallelizedJobs; job++)
                {
                    FileCounter++;
                    int fileCounter = FileCounter;
                    if ((fileCounter - 1) * MaxBatchSize < CurrentDocCount)
                    {
                        Console.WriteLine("  Backing up source documents to {0} - (batch size = {1})", BackupDirectory + "\\" + SourceIndexName + fileCounter + ".json", MaxBatchSize);

                        tasks.Add(Task.Factory.StartNew(() =>
                            ExportToJSON((fileCounter - 1) * MaxBatchSize, IDFieldName, BackupDirectory + "\\" + SourceIndexName + fileCounter + ".json")
                        ));
                    }

                }
                Task.WaitAll(tasks.ToArray());  // Wait for all the stored procs in the group to complete
            }

            return;
        }

        static void ExportToJSON(int Skip, string IDFieldName, string FileName)
        {
            // Extract all the documents from the selected index to JSON files in batches of 500 docs / file
            string json = string.Empty;
            try
            {
                SearchOptions options = new SearchOptions()
                {
                    SearchMode = SearchMode.All,
                    Size = MaxBatchSize,
                    Skip = Skip
                };

                SearchResults<SearchDocument> response = SourceSearchClient.Search<SearchDocument>("*", options);

                foreach (var doc in response.GetResults())
                {
                    json += JsonSerializer.Serialize(doc.Document) + ",";
                    json = json.Replace("\"Latitude\":", "\"type\": \"Point\", \"coordinates\": [");
                    json = json.Replace("\"Longitude\":", "");
                    json = json.Replace(",\"IsEmpty\":false,\"Z\":null,\"M\":null,\"CoordinateSystem\":{\"EpsgId\":4326,\"Id\":\"4326\",\"Name\":\"WGS84\"}", "]");
                    json += "\r\n";
                }

                // Output the formatted content to a file
                json = json.Substring(0, json.Length - 3); // remove trailing comma
                File.WriteAllText(FileName, "{\"value\": [");
                File.AppendAllText(FileName, json);
                File.AppendAllText(FileName, "]}");
                Console.WriteLine("  Total documents: {0}", response.GetResults().Count().ToString());
                json = string.Empty;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }
        }

        static string GetIDFieldName()
        {
            // Find the id field of this index
            string IDFieldName = string.Empty;
            try
            {
                var schema = SourceIndexClient.GetIndex(SourceIndexName);
                foreach (var field in schema.Value.Fields)
                {
                    if (field.IsKey == true)
                    {
                        IDFieldName = Convert.ToString(field.Name);
                        break;
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }

            return IDFieldName;
        }

        static string GetIndexSchema(String sourceIndex)
        {

            // Extract the schema for this index
            // We use REST here because we can take the response as-is

            Uri ServiceUri = new Uri("https://" + SourceSearchServiceName + ".search.windows.net");
            HttpClient HttpClient = new HttpClient();
            HttpClient.DefaultRequestHeaders.Add("api-key", SourceAdminKey);

            string Schema = string.Empty;
            Uri uri = new Uri(ServiceUri, "/indexes");
            try
            {
                if (sourceIndex == "All")
                {
                    uri = new Uri(ServiceUri, "/indexes/");
                }
                else
                {
                    uri = new Uri(ServiceUri, "/indexes/" + sourceIndex);
                }
                //get all indexes

                HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Get, uri);
                AzureSearchHelper.EnsureSuccessfulSearchResponse(response);
                Schema = response.Content.ReadAsStringAsync().Result.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }

            return Schema;
        }

        private static bool DeleteIndex(String tIndexName)
        {
            Console.WriteLine("\n  Delete target index {0} in {1} search service, if it exists", tIndexName, TargetSearchServiceName);
            // Delete the index if it exists
            try
            {
                TargetIndexClient.DeleteIndex(tIndexName);
            }
            catch (Exception ex)
            {
                Console.WriteLine("  Error deleting index: {0}\r\n", ex.Message);
                Console.WriteLine("  Did you remember to set your SearchServiceName and SearchServiceApiKey?\r\n");
                return false;
            }

            return true;
        }

        static void CreateTargetIndex(string targetIndex)
        {
            Console.WriteLine("\n  Create target index {0} in {1} search service", targetIndex, TargetSearchServiceName);
            // Use the schema file to create a copy of this index
            // I like using REST here since I can just take the response as-is
            /*
            string json = File.ReadAllText(BackupDirectory + "\\" + SourceIndexName + ".schema");

            // Do some cleaning of this file to change index name, etc
            json = "{" + json.Substring(json.IndexOf("\"name\""));
            int indexOfIndexName = json.IndexOf("\"", json.IndexOf("name\"") + 5) + 1;
            int indexOfEndOfIndexName = json.IndexOf("\"", indexOfIndexName);
            json = json.Substring(0, indexOfIndexName) + TargetIndexName + json.Substring(indexOfEndOfIndexName);
            */
            Uri ServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
            HttpClient HttpClient = new HttpClient();
            HttpClient.DefaultRequestHeaders.Add("api-key", TargetAdminKey);

            try
            {
                Uri uri = new Uri(ServiceUri, "/indexes");
                HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Post, uri, targetIndex);
                response.EnsureSuccessStatusCode();
            }
            catch (Exception ex)
            {
                Console.WriteLine("  Error: {0}", ex.Message.ToString());
            }
        }

        static int GetCurrentDocCount(SearchClient searchClient)
        {
            // Get the current doc count of the specified index
            try
            {
                SearchOptions options = new SearchOptions()
                {
                    SearchMode = SearchMode.All,
                    IncludeTotalCount = true
                };

                SearchResults<Dictionary<string, object>> response = searchClient.Search<Dictionary<string, object>>("*", options);
                return Convert.ToInt32(response.TotalCount);
            }
            catch (Exception ex)
            {
                Console.WriteLine("  Error: {0}", ex.Message.ToString());
            }

            return -1;
        }

        static void ImportFromJSON()
        {
            Console.WriteLine("\n  Upload index documents from saved JSON files");
            // Take JSON file and import this as-is to target index
            Uri ServiceUri = new Uri("https://" + TargetSearchServiceName + ".search.windows.net");
            HttpClient HttpClient = new HttpClient();
            HttpClient.DefaultRequestHeaders.Add("api-key", TargetAdminKey);

            try
            {
                foreach (string fileName in Directory.GetFiles(BackupDirectory, SourceIndexName + "*.json"))
                {
                    Console.WriteLine("  -Uploading documents from file {0}", fileName);
                    string json = File.ReadAllText(fileName);
                    Uri uri = new Uri(ServiceUri, "/indexes/" + TargetIndexName + "/docs/index");
                    HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Post, uri, json);
                    response.EnsureSuccessStatusCode();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("  Error: {0}", ex.Message.ToString());
            }
        }
    }
}
