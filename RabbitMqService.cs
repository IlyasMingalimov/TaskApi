using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client;

namespace TaskApi;

public interface IRabbitMqService : IAsyncDisposable
{
    void PublishTaskCompleted(TaskItem task);
}

public class RabbitMqService : IRabbitMqService
{
    private const string RoutingKey = "task.completed";

    private readonly ILogger<RabbitMqService> _logger;
    private readonly ConnectionFactory _factory;
    private readonly string _exchange;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    // Публикации, которые уже начались, но ещё не завершились.
    // Нужны, чтобы при graceful shutdown дождаться их отправки (таймаут 5с).
    private readonly ConcurrentBag<Task> _inFlight = new();

    private IConnection? _connection;
    private IChannel? _channel;

    public RabbitMqService(IConfiguration configuration, ILogger<RabbitMqService> logger)
    {
        _logger = logger;
        var section = configuration.GetSection("RabbitMQ");

        _factory = new ConnectionFactory
        {
            HostName = section["Host"]!,
            Port = section.GetValue<int>("Port"),
            UserName = section["UserName"]!,
            Password = section["Password"]!,
            AutomaticRecoveryEnabled = true
        };

        _exchange = section["Exchange"]!;
    }

    /// <summary>
    /// Публикует сообщение о завершении задачи в обменник <c>task.events</c>.
    /// Не блокирует основной поток: публикация уходит в фоновый пул.
    /// Если RabbitMQ недоступен — задача всё равно считается завершённой,
    /// а ошибка только пишется в лог (fail silently, но с логом).
    /// </summary>
    public void PublishTaskCompleted(TaskItem task)
    {
        var message = new TaskCompletedEvent(task.Id, task.Title, task.CompletedAt!.Value, task.Priority);
        var body = JsonSerializer.SerializeToUtf8Bytes(message);

        var publishTask = PublishAsync(body);
        _inFlight.Add(publishTask);
    }

    private async Task PublishAsync(ReadOnlyMemory<byte> body)
    {
        try
        {
            var channel = await GetChannelAsync();
            await channel.BasicPublishAsync(_exchange, RoutingKey, body);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Не удалось опубликовать сообщение в RabbitMQ (exchange={Exchange}, routingKey={RoutingKey})",
                _exchange, RoutingKey);
        }
    }

    // Соединение создаётся лениво при первой публикации и переиспользуется.
    // Если RabbitMQ лежит — CreateConnectionAsync бросит, ошибка уйдёт в лог,
    // а следующая публикация попробует снова.
    private async Task<IChannel> GetChannelAsync()
    {
        if (_channel is not null)
            return _channel;

        await _connectionLock.WaitAsync();
        try
        {
            if (_channel is not null)
                return _channel;

            _connection = await _factory.CreateConnectionAsync();
            _channel = await _connection.CreateChannelAsync();
            await _channel.ExchangeDeclareAsync(_exchange, ExchangeType.Topic, durable: true);
            return _channel;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    /// <summary>
    /// Graceful shutdown: ждём завершения начатых публикаций (до 5 секунд),
    /// затем корректно закрываем канал и соединение.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        var timeout = TimeSpan.FromSeconds(5);
        try
        {
            await Task.WhenAll(_inFlight).WaitAsync(timeout);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Не все публикации RabbitMQ завершились за {Timeout:g}", timeout);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Ошибка при ожидании завершения публикаций RabbitMQ");
        }

        if (_channel is not null)
        {
            try { await _channel.CloseAsync(); }
            catch (Exception ex) { _logger.LogWarning(ex, "Ошибка при закрытии канала RabbitMQ"); }
            await _channel.DisposeAsync();
        }

        if (_connection is not null)
        {
            try { await _connection.CloseAsync(); }
            catch (Exception ex) { _logger.LogWarning(ex, "Ошибка при закрытии соединения RabbitMQ"); }
            await _connection.DisposeAsync();
        }
    }
}

/// <summary>Сообщение, публикуемое в RabbitMQ при завершении задачи.</summary>
public record TaskCompletedEvent(
    Guid TaskId,
    string Title,
    DateTimeOffset CompletedAt,
    [property: JsonConverter(typeof(JsonStringEnumConverter))] Priority Priority);
