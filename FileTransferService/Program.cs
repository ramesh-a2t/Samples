namespace FileTransferService
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = Host.CreateApplicationBuilder(args);
            builder.Services.AddHostedService<Worker>();
            builder.Services.AddApplicationInsightsTelemetryWorkerService();

            var host = builder.Build();
            host.Run();
        }
    }
}