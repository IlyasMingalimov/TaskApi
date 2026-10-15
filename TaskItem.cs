namespace TaskApi;

public class TaskItem
{
    public Guid Id { get; set; }
    public string Title { get; set; }
    public bool IsCompleted { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; } // заполняется только в момент завершения
    public Priority Priority { get; set; } // Low, Medium, High

    /// <summary>RowVersion (xmin в PostgreSQL) — для оптимистичной блокировки при конкурентном завершении.</summary>
    public uint RowVersion { get; set; }
}
