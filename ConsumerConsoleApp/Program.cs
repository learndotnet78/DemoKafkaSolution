using Confluent.Kafka;
using Newtonsoft.Json;

var config = new ConsumerConfig
{
    GroupId = "weather-consume-group",
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

var consumer = new ConsumerBuilder<Null, string>(config).Build();

consumer.Subscribe("weather-topic");

CancellationToken token = new();


try
{
    string? state;
    while (true)
    {
        var response = consumer.Consume(token);
        if (response != null)
        {
            var weather = JsonConvert.DeserializeObject<Weather>(response.Message.Value);
            if (weather != null)
            {
                Console.WriteLine($"State - {weather.State} and Temperature - {weather.Temperature}F");
            }
        }
    }
}
catch (ProduceException<Null,string> ex)
{
    Console.WriteLine(ex.Message);
}

public record Weather(string State, int Temperature);
