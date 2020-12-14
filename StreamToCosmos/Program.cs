using System;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Generic;
using System.Net;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using System.Reflection;
using System.IO;
using System.Threading;

namespace StreamToCosmos
{
    class Program
    {
        // The Azure Cosmos DB endpoint for running this sample.
        private static readonly string EndpointUri = "<end point URL>";
        // The primary key for the Azure Cosmos account.
        private static readonly string PrimaryKey = "<primarykey>";

        // The Cosmos client instance
        private CosmosClient cosmosClient;

        // The database we will create
        private Database database;

        // The container we will create.
        private Container container;

        // The name of the database and container we will create
        private string databaseName = "TestDatabase";
        private string containerName = "CovidData";

        static async Task Main(string[] args)
        {
            try
            {
                List<CosmosItem> items = new List<CosmosItem>();

                Program p = new Program();
                p.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey);
                p.database = await p.cosmosClient.CreateDatabaseIfNotExistsAsync(p.databaseName);
                Console.WriteLine("Created Database: {0}\n", p.database.Id);
                p.container = await p.database.CreateContainerIfNotExistsAsync(p.containerName, "/country");
                Console.WriteLine("Created Container: {0}\n", p.container.Id);


                var assembly = Assembly.GetExecutingAssembly();
                var resourceName = "StreamToCosmos.ecdc_cases.json";

                using (Stream stream = assembly.GetManifestResourceStream(resourceName))
                using (StreamReader reader = new StreamReader(stream))
                {
                    string result = reader.ReadToEnd();
                    items = JsonConvert.DeserializeObject<List<CosmosItem>>(result);
                }

                foreach(var item in items)
                {
                    item.Id = Guid.NewGuid().ToString();
                    await p.container.CreateItemAsync<CosmosItem>(item);
                    Console.WriteLine("Created Document: {0}\n", item.Id);
                    Thread.Sleep(1000);
                }
            }
            catch (CosmosException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}", de.StatusCode, de);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }

        public class CosmosItem
        {
            [JsonProperty("id")]
            public string Id { get; set; }
            [JsonProperty("date_rep")]
            public string Date_rep { get; set; }
            [JsonProperty("day")]
            public string Day { get; set; }
            [JsonProperty("month")]
            public string Month { get; set; }
            [JsonProperty("year")]
            public string Year { get; set; }
            [JsonProperty("cases")]
            public int Cases { get; set; }
            [JsonProperty("deaths")]
            public int Deaths { get; set; }
            [JsonProperty("countries_and_territories")]
            public string CountriesTerritories { get; set; }
            [JsonProperty("geo_id")]
            public string GroId { get; set; }
            [JsonProperty("country_territory_code")]
            public string CountryTerritoryCode { get; set; }
            [JsonProperty("continent_exp")]
            public string Continent { get; set; }
            [JsonProperty("load_date")]
            public string LoadDate { get; set; }
            [JsonProperty("iso_country")]
            public string Country { get; set; }
        }
    }
}
