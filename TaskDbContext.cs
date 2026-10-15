using Microsoft.EntityFrameworkCore;

namespace TaskApi;

public class TaskDbContext(DbContextOptions<TaskDbContext> options) : DbContext(options)
{
    public DbSet<TaskItem> Tasks => Set<TaskItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TaskItem>(entity =>
        {
            entity.ToTable("tasks");
            entity.HasKey(t => t.Id);
            entity.Property(t => t.Title)
                .IsRequired()
                .HasMaxLength(200);
            // RowVersion: в PostgreSQL маппится на системный столбец xmin,
            // который меняется при каждом UPDATE — это и даёт 409 при гонке.
            entity.Property(t => t.RowVersion)
                .IsRowVersion();
        });
    }
}
