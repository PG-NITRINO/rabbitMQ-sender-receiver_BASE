using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Receiver
{
    internal class Consumer
    {
        private string? _hostName { get; set; }
        private string? _userName { get; set; }
        private string? _password { get; set; }
        private string? _virtualHost { get; set; }
        private int _port { get; set; }

        //В конструкторе указываем свойства подключения, по умолчанию это стандартное подключение на локальном хосте без виртуализации
        public Consumer(string hostName = "localhost", string userName = "guest", string password = "guest", int port = 5672, string virtualHost = "/")
        {
            _hostName = hostName;
            _userName = userName;
            _password = password;
            _virtualHost = virtualHost;
            _port = port;

            _factory = null;
            _consumerConnection = null;
            _consumerChannel = null;
            _queueName = null;
            _lastException = String.Empty;
        }

        //Хранит текст последнего исключения
        private string? _lastException { get; set; }

        //Получить сообщение последнего исключения
        public string? GetLastException()
        {
            return _lastException;
        }

        private ConnectionFactory? _factory;
        private IConnection? _consumerConnection;
        private IModel? _consumerChannel;
        private string? _queueName;

        //Создание подключения
        //Создание канала
        //Подключение к указанной очереди с данными свойствами
        public bool Connection(string QueueName, bool Queue_durable = false, bool Queue_exclusive = false,
                                    bool Queue_autoDelete = false, IDictionary<string, object>? Queue_arguments = null)
        {
            _queueName = QueueName;
            //Фабрика соединений классов - Основная точка входа в клиентский API RabbitMQ .NET AMQP. Создает экземпляры IConnection .
            //IConnection: представляет соединение AMQP 0 - 9 - 1.
            _factory = new ConnectionFactory();
            _factory.HostName = _hostName;
            _factory.UserName = _userName;
            _factory.Password = _password;
            _factory.Port = _port;
            _factory.VirtualHost = _virtualHost;

            //Aвтоматическое восстановление подключения
            _factory.AutomaticRecoveryEnabled = true;
            //Попытка восстановить соединение через 10 секунд, по стандарту - 5
            _factory.NetworkRecoveryInterval = TimeSpan.FromSeconds(10);

            try
            {
                //Создаёт подключение к одной из конечных точек, предоставленных IEndpointResolver, возвращаемым EndpointResolverFactory.
                //По умолчанию используются настроенные имя хоста и порт.
                _consumerConnection = _factory.CreateConnection();
                
            }
            catch (RabbitMQ.Client.Exceptions.BrokerUnreachableException exc)
            {
                //Возникает, когда не удается открыть соединение во время попытки ConnectionFactory.CreateConnection.
                _factory = null;
                _consumerConnection = null;
                _queueName = String.Empty;
                _lastException = "type: " + exc.GetType() + ", message: " +exc.Message;
                return false;
            }

            try
            {
                //Открытие канала
                _consumerChannel = _consumerConnection.CreateModel();
            }
            catch (RabbitMQ.Client.Exceptions.ChannelAllocationException exc)
            {
                //Генерируется, когда SessionManager не может выделить новый номер канала или запрошенный номер канала уже используется.
                //Соеденение _consumerConnection уничтожается
                _factory = null;
                _consumerConnection = null;
                _consumerChannel = null;
                _queueName = String.Empty;
                _lastException = "type: " + exc.GetType() + ", message: " + exc.Message;
                return false;
            }

            try
            {
                //durable — если true, то очередь будет сохранять свое состояние и восстанавливается после перезапуска сервера/брокера
                //exclusive — если true, то очередь будет разрешать подключаться только одному потребителю
                //autoDelete — если true, то очередь обретает способность автоматически удалять себя
                _consumerChannel.QueueDeclare(queue: QueueName,
                                     durable: Queue_durable,
                                     exclusive: Queue_exclusive,
                                     autoDelete: Queue_autoDelete,
                                     arguments: Queue_arguments);

                //Дополнителельные настройки канала
                //prefetchSize — размер окна предварительной выборки, если = 0, то «без определенного ограничения»
                //prefetchCount - окно предварительной выборки с точки зрения целых сообщений.
                //global — должно ли QoS совместно использоваться всеми потребителями на канале.
                _consumerChannel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            }
            catch (RabbitMQ.Client.Exceptions.OperationInterruptedException exc)
            {
                //Генерируется при уничтожении сеанса во время вызова RPC к брокеру. Например, если разрыв соединения TCP вызывает уничтожение сеанса в середине операции QueueDeclare, вызывающей стороне IModel.QueueDeclare будет выдано исключение OperationInterruptedException.
                //Соеденение _consumerConnection уничтожается
                _factory = null;
                _consumerConnection = null;
                _consumerChannel = null;
                _queueName = String.Empty;
                _lastException = "type: " + exc.GetType() + ", message: " + exc.Message + ", ShutdownReason: " + exc.ShutdownReason;
                return false;
            }
            
            return true;
        }

        //Стандартный вариант - сообщения читаются из очереди и выводятся в консоль
        public bool StartStandartConsumer()
        {
            if (_consumerChannel != null)
            {
                var consumer = new EventingBasicConsumer(_consumerChannel);
                consumer.Received += (model, ea) =>
                {
                    byte[] body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($" [x] Received {message}");

                    // здесь к каналу также можно было бы получить доступ как к отправителю
                    _consumerChannel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };
 ///ВОПРОС ВСЕГДА ЛИ AUTO ACK FALSE?????
                _consumerChannel.BasicConsume(queue: _queueName,
                                     autoAck: false,
                                     consumer: consumer);

                return true;
            }

            _lastException = "No connection";
            return false;
        }

        public IModel? GetChannel()
        {
            return _consumerChannel;
        }

        //Пользовательский вариант обработчика сообщений
        public bool StartCustomConsumer(EventHandler<BasicDeliverEventArgs> cc_func)
        {
            if(_consumerChannel != null)
            {
                var consumer = new EventingBasicConsumer(_consumerChannel);
                consumer.Received += cc_func;
                _consumerChannel.BasicConsume(queue: _queueName,
                                     autoAck: false,
                                     consumer: consumer);

                return true;
            }

            _lastException = "No connection";
            return false;
        }

        public bool Close()
        {
            try
            {
                if (_consumerChannel != null) _consumerChannel.Close();
                if (_consumerConnection != null) _consumerConnection.Close();
            }
            catch (RabbitMQ.Client.Exceptions.AlreadyClosedException exc)
            {
                //Генерируется, когда приложение пытается использовать сеанс или соединение, которое уже было закрыто.
                _lastException = "type: " + exc.GetType() + ", message: " + exc.Message + ", ShutdownReason: " + exc.ShutdownReason;
                return false;
            }
            finally
            {
                _factory = null;
                _consumerConnection = null;
                _consumerChannel = null;
                _queueName = String.Empty;
            }

            return true;
        }

        //Открыто ли соединение
        public bool IsConnected
        {
            get
            {
                return _consumerConnection != null && _consumerConnection.IsOpen;
            }
        }

        ~Consumer()
        {
            Close();
        }        
    }
}
