using System.Text;
using MediatR;
using MicroRabbit.Domain.Core.Bus;
using MicroRabbit.Domain.Core.Commands;
using MicroRabbit.Domain.Core.Events;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Linq;
using RabbitMQ.Client.Events;

namespace MicroRabbit.Infra.Bus;

public sealed class RabbitMQBus : IEventBus
{
    private readonly IMediator _mediator;
    private readonly Dictionary<string, List<Type>>_eventHandlers;
    private readonly List<Type> _eventTypes; 
    
    public RabbitMQBus(IMediator mediator)
    {
        _mediator = mediator;
        _eventHandlers = new Dictionary<string,  List<Type>>();
        _eventTypes = new List<Type>();
    }
    
    public Task SendCommand<T>(T command) where T : Command
    {
        return _mediator.Send(command);
    }
    
    public void Publish<T>(T @event) where T : Event
    {
        var factory = new ConnectionFactory(){ HostName = "localhost" };
        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            var eventName = @event.GetType().Name;
            channel.QueueDeclare(eventName, false, false, false, null);

            var message = JsonConvert.SerializeObject(@event);
            var body = Encoding.UTF8.GetBytes(message);
            
            channel.BasicPublish("",eventName, null, body);
        }
    }

    public void Subscribe<T, TH>() where T : Event where TH : IEventHandler<T>
    {
        var eventName = typeof(T).Name;
        var handlerType = typeof(TH);

        if (!_eventTypes.Contains(typeof(T)))
        {
            _eventTypes.Add(typeof(T));
        }

        if (!_eventHandlers.ContainsKey(eventName))
        {
            _eventHandlers.Add(eventName, new List<Type>());
        }

        if (_eventHandlers[eventName].Any((x) => x.GetType() == handlerType))
        {
            throw new ArgumentException(
                $"Handler Type {handlerType.Name} already is registered for '{eventName}'", nameof(handlerType));
        }
        
        _eventHandlers[eventName].Add(handlerType);
        
        StartBasicConsume<T>();
    }
    
    private void StartBasicConsume<T>() where T : Event
    {
        var factory = new ConnectionFactory()
        {
            HostName = "localhost",
            DispatchConsumersAsync = true
        };
        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            var eventName = typeof(T).Name;
            channel.QueueDeclare(eventName, false, false, false, null);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += ConsumerReceived;
            channel.BasicConsume(eventName, true, consumer);
        }
        
    }

    private async Task ConsumerReceived(object sender, BasicDeliverEventArgs e)
    {
        var eventName = e.RoutingKey;
        var message = Encoding.UTF8.GetString(e.Body);
        try
        {
            await ProcessEvent(eventName, message).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception);
            throw;
        }
    }

    private async Task ProcessEvent(string eventName, string message)
    {
       if (_eventHandlers.ContainsKey(eventName))
       {
           var subscriptions = _eventHandlers[eventName];
           foreach (var subscription in subscriptions)
           {
               var handler = Activator.CreateInstance(subscription);
               if (handler == null) continue;
               var eventType = _eventTypes.SingleOrDefault(x => x.Name == eventName);
               var @event = JsonConvert.DeserializeObject(message, eventType);
               var concreteType = typeof(IEventHandler<>).MakeGenericType(eventType);
               await (Task)concreteType.GetMethod("Handle").Invoke(handler, new[] { @event });
           }
       }
    }
}