using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace FileTransferService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        private readonly TelemetryClient _telemetryClient;
        private FileSystemWatcher _trxWatcher = null!;
        private FileSystemWatcher _imagesWatcher = null!;
        private string _trxFolder = null!;
        private string _imagesFolder = null!;
        private string _blobConnectionString = null!;
        private string _blobContainer = null!;
        private BlobContainerClient _blobContainerClient = null!;
        private bool _debug;

        public Worker(ILogger<Worker> logger, IConfiguration configuration, TelemetryConfiguration telemetryConfiguration)
        {
            _logger = logger;
            _configuration = configuration;
            _telemetryClient = new TelemetryClient(telemetryConfiguration);
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("FileTransfer Worker starting...");
            _telemetryClient.TrackEvent("FileTransferWorkerStarted");

            // Load configuration values
            _trxFolder = _configuration["Folders:TrxData"]!;
            _imagesFolder = _configuration["Folders:Images"]!;
            _blobConnectionString = _configuration["BlobStorage:ConnectionString"]!;
            _blobContainer = _configuration["BlobStorage:Container"]!;
            _debug = _configuration.GetValue<bool>("Debug:EnableTiming");

            if (!Directory.Exists(_trxFolder) || !Directory.Exists(_imagesFolder))
            {
                _logger.LogCritical("Configured folders do not exist or cannot be accessed.");
                _telemetryClient.TrackException(new DirectoryNotFoundException("One or more configured directories are inaccessible."));
                throw new Exception("Cannot access configured directories.");
            }

            // Initialize Azure Blob Storage Client with AAD Authentication
            var credential = new DefaultAzureCredential();
            var blobServiceClient = new BlobServiceClient(new Uri(_configuration["BlobStorage:AccountUrl"]!), credential);
            _blobContainerClient = blobServiceClient.GetBlobContainerClient(_blobContainer);

            // Initialize File Watchers
            _trxWatcher = CreateWatcher(_trxFolder);
            _imagesWatcher = CreateWatcher(_imagesFolder);

            return base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            stoppingToken.Register(() =>
                _logger.LogInformation("FileTransfer Worker stopping..."));

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    await Task.Delay(1000, stoppingToken); // Keep service alive
                }
            }
            catch (TaskCanceledException)
            {
                // Graceful shutdown
            }
        }

        private FileSystemWatcher CreateWatcher(string folderPath)
        {
            var watcher = new FileSystemWatcher(folderPath)
            {
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite,
                EnableRaisingEvents = true,
                IncludeSubdirectories = false
            };

            watcher.Created += async (s, e) => await ProcessFile(e.FullPath);
            return watcher;
        }

        private async Task ProcessFile(string filePath)
        {
            try
            {
                var fileName = Path.GetFileName(filePath);
                var fileDate = File.GetLastWriteTime(filePath);
                var blobPath = Path.Combine(fileDate.ToString("yyyy/MM/dd"), fileName);

                if (_configuration.GetValue<bool>("BlobStorage:UseHourlyOrganization"))
                {
                    blobPath = Path.Combine(blobPath, fileDate.ToString("HH"), fileName);
                }

                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                // Upload to Azure Blob Storage
                var blobClient = _blobContainerClient.GetBlobClient(blobPath);

                await using (var fileStream = File.OpenRead(filePath))
                {
                    await blobClient.UploadAsync(fileStream, true);
                }

                stopwatch.Stop();

                if (_debug)
                {
                    _logger.LogDebug($"Uploaded file {fileName} in {stopwatch.ElapsedMilliseconds}ms");
                }

                _logger.LogInformation($"Successfully uploaded {fileName} to {blobPath}");
                _telemetryClient.TrackEvent("FileUploaded", new Dictionary<string, string> { { "FileName", fileName }, { "BlobPath", blobPath }});
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error processing file {filePath}");
                _telemetryClient.TrackException(ex);
            }
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("FileTransfer Worker stopping...");
            _telemetryClient.TrackEvent("FileTransferWorkerStopped");

            _trxWatcher?.Dispose();
            _imagesWatcher?.Dispose();

            return base.StopAsync(cancellationToken);
        }
    }
}
