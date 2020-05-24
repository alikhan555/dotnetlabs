using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace EmailWorker
{
    internal class Program
    {
        private static void Main()
        {
            var factory = new ConnectionFactory
            {
                UserName = "ops1",
                Password = "ops1"
            };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            const string queueName = "emailworker";
            channel.QueueDeclarePassive(queueName);
            Console.WriteLine($"Queue [{queueName}] is waiting for messages.");

            var messageCount = channel.MessageCount(queueName);
            if (messageCount > 0)
            {
                Console.WriteLine($"\tDetected {messageCount} message(s).");
            }

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (bc, ea) =>
            {
                if (ea.BasicProperties.UserId != "ops0")
                {
                    Console.WriteLine($"\tIgnored a message sent by [{ea.BasicProperties.UserId}].");
                    return;
                }

                try
                {
                    var t = DateTimeOffset.FromUnixTimeMilliseconds(ea.BasicProperties.Timestamp.UnixTime);
                    Console.WriteLine($"{t.LocalDateTime:O} ID=[{ea.BasicProperties.MessageId}]");
                    var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                    var order = JsonSerializer.Deserialize<Order>(message);
                    Console.WriteLine($"\tProcessing order: '{message}'.");
                    Thread.Sleep((new Random().Next(1, 3)) * 1000);
                    Console.WriteLine($"\tOrder #[{order.Id} confirmation sent to [{order.Email}].");
                    var model = ((IBasicConsumer)bc).Model;
                    model.BasicAck(ea.DeliveryTag, false);
                }
                catch (AlreadyClosedException)
                {
                    Console.WriteLine("RabbitMQ is closed!");
                }
            };
            channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

            connection.Close();
        }
    }

    public class Order
    {
        public int Id { get; set; }
        public string Email { get; set; }
    }
}
