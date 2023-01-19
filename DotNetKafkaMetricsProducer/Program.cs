// See https://aka.ms/new-console-template for more information

using Com.RFranco.Kafka.Statistics;
using Com.RFranco.Kafka.Statistics.Prometheus;
using Confluent.Kafka;
using DotNetKafkaMetricsProducer.Config;
using Microsoft.Extensions.Configuration;
using Prometheus;

var configuration = GetConfiguration(args);

try
{
    Producer(configuration);
}
catch (Exception ex)
{
    Console.WriteLine($"An error occurred while starting up the test. {ex}");
    Environment.Exit(-2);
}

Console.WriteLine("Hello, World!");

void Producer(IConfiguration configuration1)
{
    var prometheusConfig = configuration1.GetSection("prometheusMetrics").Get<PrometheusConfig>();

    MetricServer metricServer;
    
    metricServer = new MetricServer(port: prometheusConfig.Port);
    metricServer.Start(); //once started you can check by hitting http://localhost:9096/metrics 

    CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

    Dictionary<string, string> dicConfig = new Dictionary<string, string>
    {
        { "bootstrap.servers", "{your-broker-here}" },
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanisms", "PLAIN" },
        { "sasl.username", "{you username here}" },
        { "sasl.password", "{your password here}" },
        { "session.timeout.ms", "45000" }
    };

    var clientConfig = new ClientConfig(dicConfig);

    ProducerBuilder<Null, string> builder = new ProducerBuilder<Null, string>(clientConfig);

    builder.SetErrorHandler((_, error) =>
    {
        Console.WriteLine($"An error ocurred producing the event: {error.Reason}");
        if (error.IsFatal) Environment.Exit(-1);
    });

    builder.HandleStatistics(new PrometheusProducerStatisticsHandler(new string[] { "application" },
        new string[] { "test-producer-statistics" }));
    builder.SetKeySerializer(Serializers.Null);
    builder.SetValueSerializer(Serializers.Utf8);


    using (var producer = builder.Build())
    {
        Action<DeliveryReport<Null, string>> handler = r =>
        {
            if (r.Error.IsError)
            {
                Console.WriteLine($"Delivery Error: {r.Error.Reason}");
            }
            else
            {
                Console.WriteLine($"Delivered message to {r.TopicPartitionOffset}");
            }
        };

        int numMessages = 0;
        while (!cancellationTokenSource.IsCancellationRequested)
        {
            try
            {
                var dr = producer.ProduceAsync(configuration1.GetValue<string>("topic"),
                    new Message<Null, string> { Value = $"message {numMessages}" });
                Console.WriteLine($"Delivered  message {numMessages} : {dr.Result.Value}");
                Thread.Sleep(1000);
                numMessages++;
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }

        Console.WriteLine("Exit requested.");
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    Console.WriteLine("Exit requested. Gracefully exiting...");
}

IConfiguration GetConfiguration(string[] args)
{
    var configurationBuilder = new ConfigurationBuilder();
    
    configurationBuilder.AddJsonFile("config.json", optional: true, reloadOnChange: true);
    configurationBuilder.AddEnvironmentVariables();
    configurationBuilder.AddCommandLine(args);
    return configurationBuilder.Build();
}