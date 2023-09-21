// See https://aka.ms/new-console-template for more information

using Com.RFranco.Kafka.Statistics;
using Com.RFranco.Kafka.Statistics.Prometheus;
using Confluent.Kafka;
using DotNetKafkaMetricsProducer.Config;
using Microsoft.Extensions.Configuration;
using Prometheus;
using Serilog;

var configuration = GetConfiguration(args);
IConfiguration Configuration;

Configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .Build();
             
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(Configuration)
    .WriteTo.Console()
    .CreateLogger();

try
{
    Producer(configuration);
}
catch (Exception ex)
{
    Log.Logger.Error($"An error occurred while starting up the test. {ex}");
    Environment.Exit(-2);
}



void Producer(IConfiguration configuration1)
{
    var prometheusConfig = configuration1.GetSection("prometheusMetrics").Get<PrometheusConfig>();

    MetricServer metricServer;
    
    metricServer = new MetricServer(port: prometheusConfig.Port);
    metricServer.Start(); //once started you can check by hitting http://localhost:9096/metrics 

    CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

    var config = new ProducerConfig
    {
        Partitioner = Partitioner.Murmur2Random, //important this is same as java, so if doing kstreams or ksqldb then you must use same partition algorithm as them
        SecurityProtocol = SecurityProtocol.SaslSsl,
        SaslMechanism = SaslMechanism.Plain,
        SaslUsername = "",
        SaslPassword = "",
        BootstrapServers = ""
    };

    var clientConfig = new ClientConfig(config);
    
    clientConfig.Debug = "broker,topic,msg";
        
    ProducerBuilder<Null, string> builder = new ProducerBuilder<Null, string>(clientConfig);

    builder.SetErrorHandler((_, error) =>
    {
        Log.Logger.Error($"An error ocurred producing the event: {error.Reason}");
        if (error.IsFatal) Environment.Exit(-1);
    });

    builder.SetLogHandler((_, message) =>
    {
        Log.Logger.Debug(message.Message);
    });
    
    builder.HandleStatistics(new PrometheusProducerStatisticsHandler(new string[] { "application" },
        new string[] { "test-producer-statistics" }));
    builder.SetKeySerializer(Serializers.Null);
    builder.SetValueSerializer(Serializers.Utf8);


    using (var producer = builder.Build())
    {
        int numMessages = 0;
        while (!cancellationTokenSource.IsCancellationRequested)
        {
            try
            {
                var topic = configuration1.GetValue<string>("topic");
                var dr = producer.ProduceAsync(topic,
                    new Message<Null, string> { Value = $"message {numMessages}" });
                Log.Logger.Information($"Delivered  message {numMessages} : {dr.Result.Value}");
                
                numMessages++;
            }
            catch (ProduceException<Null, string> e)
            {
                Log.Logger.Error($"Delivery failed: {e.Error.Reason}");
            }
        }

        Log.Logger.Information("Exit requested.");
        
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    Log.Logger.Information("Exit requested. Gracefully exiting...");
}

IConfiguration GetConfiguration(string[] args)
{
    var configurationBuilder = new ConfigurationBuilder();
    
    configurationBuilder.AddJsonFile("config.json", optional: true, reloadOnChange: true);
    configurationBuilder.AddEnvironmentVariables();
    configurationBuilder.AddCommandLine(args);
    return configurationBuilder.Build();
}