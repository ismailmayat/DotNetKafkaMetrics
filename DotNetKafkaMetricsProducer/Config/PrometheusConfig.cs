namespace DotNetKafkaMetricsProducer.Config;

public class PrometheusConfig
{
    public PrometheusConfig()
    {
        Enabled = false;
        Port = 1234;
    }

    public bool Enabled { get; set; }
    public int Port { get; set; }
}