using System.Text;
using RabbitMQ.Client;

internal class SenderProgram
{
    static void Main(string[] args)
    {
        //var factory = new ConnectionFactory { HostName = "localhost" };

        //Фабрика соединений классов - Основная точка входа в клиентский API RabbitMQ .NET AMQP. Создает экземпляры IConnection .
        //IConnection: представляет соединение AMQP 0 - 9 - 1.
        ConnectionFactory factory = new ConnectionFactory();
        factory.HostName = "localhost";
        factory.UserName = "guest";
        factory.Password = "guest";
        factory.Port = 5672;
        factory.VirtualHost = "MyVirtualHost";

        //Создаёт подключение к одной из конечных точек, предоставленных IEndpointResolver, возвращаемым EndpointResolverFactory.
        //По умолчанию используются настроенные имя хоста и порт.
        IConnection connection = factory.CreateConnection();
        //Открытие канала
        IModel channel = connection.CreateModel();

        //durable — если true, то очередь будет сохранять свое состояние и восстанавливается после перезапуска сервера/брокера
        //exclusive — если true, то очередь будет разрешать подключаться только одному потребителю
        //autoDelete — если true, то очередь обретает способность автоматически удалять себя
        channel.QueueDeclare(queue: "Bet",
                             durable: false,
                             exclusive: false,
                             autoDelete: false,
                             arguments: null);
        channel.QueueDeclare(queue: "BetSelection",
                             durable: false,
                             exclusive: false,
                             autoDelete: false,
                             arguments: null);
        channel.QueueDeclare(queue: "Bonus",
                             durable: false,
                             exclusive: false,
                             autoDelete: false,
                             arguments: null);
        channel.QueueDeclare(queue: "EventData",
                             durable: false,
                             exclusive: false,
                             autoDelete: false,
                             arguments: null);

        List<string> queues = new List<string>() { "Bet", "BetSelection", "Bonus", "EventData" };

        string? message;
        while(true)
        { 
            Console.WriteLine("Your message (or \"exit\")");
            message = Console.ReadLine();

            if (message == "exit" || message == null) break;

            foreach(var queue in queues)
            {
                string mes = queue + ": " + message;
                byte[] body = Encoding.UTF8.GetBytes(mes);

                channel.BasicPublish(exchange: string.Empty,
                             routingKey: queue,
                             basicProperties: null,
                             body: body);
            }

            Console.WriteLine($" [x] Sent {message}");
        }
        

        //var message = GetMessage(args);
        //var body = Encoding.UTF8.GetBytes(message);

        //var properties = channel.CreateBasicProperties();
        //properties.Persistent = true;

        //channel.BasicPublish(exchange: string.Empty,
        //                     routingKey: "task_queue",
        //                     basicProperties: properties,
        //                     body: body);
        //Console.WriteLine($" [x] Sent {message}");

        //Console.WriteLine(" Press [enter] to exit.");
        //Console.ReadLine();
    }

    static string GetMessage(string[] args)
    {
        return ((args.Length > 0) ? string.Join(" ", args) : "Hello World!");
    }
}