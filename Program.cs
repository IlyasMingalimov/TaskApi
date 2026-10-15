using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using TaskApi;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection");

builder.Services.AddDbContext<TaskDbContext>(options =>
    options.UseNpgsql(connectionString));

builder.Services.AddSingleton<IRabbitMqService, RabbitMqService>();

// Enum'ы в JSON как строки ("High", а не 2)
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

var app = builder.Build();

// POST /tasks — создаёт задачу (по умолчанию IsCompleted=false, Priority=Medium)
app.MapPost("/tasks", async (CreateTaskRequest request, TaskDbContext db) =>
{
    if (string.IsNullOrWhiteSpace(request.Title) || request.Title.Length > 200)
        return Results.BadRequest("Title обязателен и не должен превышать 200 символов");

    var task = new TaskItem
    {
        Title = request.Title,
        IsCompleted = false,
        Priority = request.Priority ?? Priority.Medium,
        CreatedAt = DateTimeOffset.UtcNow
        // CompletedAt заполняется только в момент завершения
    };

    db.Tasks.Add(task);
    await db.SaveChangesAsync();

    return Results.Created($"/tasks/{task.Id}", task);
});

// GET /tasks — возвращает все задачи (без пагинации)
app.MapGet("/tasks", async (TaskDbContext db) =>
    Results.Ok(await db.Tasks.AsNoTracking().ToListAsync()));

// PUT /tasks/{id}/complete — завершает задачу, ставит CompletedAt, отправляет RabbitMQ
app.MapPut("/tasks/{id}/complete", async (Guid id, TaskDbContext db, IRabbitMqService rabbit) =>
{
    var task = await db.Tasks.SingleOrDefaultAsync(t => t.Id == id);
    if (task is null)
        return Results.NotFound();

    if (task.IsCompleted)
        return Results.Conflict("Задача уже завершена");

    task.IsCompleted = true;
    task.CompletedAt = DateTimeOffset.UtcNow;

    try
    {
        await db.SaveChangesAsync();
    }
    catch (DbUpdateConcurrencyException)
    {
        // RowVersion изменился — другой запрос успел завершить задачу
        return Results.Conflict("Задача была изменена другим запросом");
    }

    // Публикуем ТОЛЬКО после успешного сохранения в БД,
    // чтобы при падении БД сообщение не ушло зря.
    rabbit.PublishTaskCompleted(task);

    return Results.Ok(task);
});

// DELETE /tasks/{id} — удаляет задачу (жёстко, без восстановления)
app.MapDelete("/tasks/{id}", async (Guid id, TaskDbContext db) =>
{
    var task = await db.Tasks.SingleOrDefaultAsync(t => t.Id == id);
    if (task is null)
        return Results.NotFound();

    db.Tasks.Remove(task);
    await db.SaveChangesAsync();

    return Results.NoContent();
});

app.Run();

// Нужен для WebApplicationFactory в интеграционных тестах
public partial class Program { }

public record CreateTaskRequest(string Title, Priority? Priority);