using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Receiver;

internal class ReceiverProgram
{
    static void Main(string[] args)
    {
        Consumer consumer = new Consumer(hostName: "localhost", userName: "guest", password: "guest", port: 5672, virtualHost: "MyVirtualHost");

        if (!consumer.Connection("BetSelection"))
        {
            Console.WriteLine("Возникло исключение: \"" + consumer.GetLastException() + "\"");
        }
        //Пример встроенного обработчика с выводом на экран
        //consumer.StartStandartConsumer();

        //Пример пользовательского обработчика, где в качестве параметра передаётся лямбда-функция
        /*consumer.StartCustomConsumer((model, ea) =>
        {
            byte[] body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            Console.WriteLine($" [x] Received {message}");

            // здесь к каналу также можно было бы получить доступ как к отправителю
            consumer.GetChannel().BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
        });*/

        //Пример пользовательского обработчика, где в качестве параметра передаётся функция
        void MyProcessor(object? model, BasicDeliverEventArgs ea)
        {
            byte[] body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            Console.WriteLine($" [x] Received {message}");

            // здесь к каналу также можно было бы получить доступ как к отправителю
            consumer.GetChannel().BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
        }
        consumer.StartCustomConsumer(MyProcessor);

        Console.WriteLine("Consumer BetSelection Start\n Press [enter] to continue.");
        Console.ReadLine();

        consumer.Close();
        Console.WriteLine("Connection BetSelection Close\n Press [enter] to continue.");
        Console.ReadLine();

        consumer.Connection("Bonus");
        consumer.StartStandartConsumer();

        Console.WriteLine("Connection Bonus Start\n Press [enter] to exit.");
        Console.ReadLine();
    }
}