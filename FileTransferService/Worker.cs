using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
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
        private string _archiveFolder = null!;
        private string _blobConnectionString = null!;
        private string _blobContainer = null!;
        private BlobContainerClient _blobContainerClient = null!;
        private bool _debug;
        private System.Timers.Timer _healthTimer = null!;
        private int _timerInterval = 300000;
        private int _processedFilesCount = 0;

        public Worker(ILogger<Worker> logger, IConfiguration configuration, TelemetryConfiguration telemetryConfiguration)
        {
            _logger = logger;
            _configuration = configuration;
            _telemetryClient = new TelemetryClient(telemetryConfiguration);
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("FileTransfer Worker starting...");
            _telemetryClient.TrackEvent("FileTransferWorkerStarted");

            // Load configuration values
            _trxFolder = _configuration["Folders:TrxData"]!;
            _imagesFolder = _configuration["Folders:Images"]!;
            _archiveFolder = _configuration["Folders:Archive"]!;
            _blobConnectionString = _configuration["BlobStorage:ConnectionString"]!;
            _blobContainer = _configuration["BlobStorage:Container"]!;
            _debug = _configuration.GetValue<bool>("Debug:EnableTiming");
            _timerInterval = _configuration.GetValue<int>("Debug:HealthTimerInterval");

            _healthTimer = new System.Timers.Timer(_timerInterval); // 5 minutes in milliseconds
            _healthTimer.Elapsed += (sender, e) => SendHealthTelemetry();
            _healthTimer.AutoReset = true;
            _healthTimer.Start();

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

            // Process existing files in the folders
            await ProcessExistingFilesAsync(_trxFolder, "trxdata");
            await ProcessExistingFilesAsync(_imagesFolder, "images");

            // Initialize File Watchers
            _trxWatcher = CreateWatcher(_trxFolder, "trxdata");
            _imagesWatcher = CreateWatcher(_imagesFolder, "images");

            await base.StartAsync(cancellationToken);
        }

        private void SendHealthTelemetry()
        {
            try
            {
                var healthData = new Dictionary<string, string>
                {
                    { "Status", "Healthy" },
                    { "Uptime", DateTime.UtcNow.Subtract(Process.GetCurrentProcess().StartTime.ToUniversalTime()).ToString(@"hh\:mm\:ss") },
                    // { "ProcessedFilesCount", _processedFilesCount.ToString() } 
                };

                _telemetryClient.TrackEvent("FileTransfer ApplicationHealth", healthData);
                _logger.LogInformation("Health telemetry sent.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send health telemetry.");
                _telemetryClient.TrackException(ex);
            }
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

        private FileSystemWatcher CreateWatcher(string folderPath, string virtualFolder)
        {
            var watcher = new FileSystemWatcher(folderPath)
            {
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite,
                EnableRaisingEvents = true,
                IncludeSubdirectories = false
            };

            watcher.Created += async (s, e) => await ProcessFile(e.FullPath, virtualFolder);
            return watcher;
        }

        private async Task ProcessExistingFilesAsync(string folderPath, string virtualFolder)
        {
            try
            {
                var files = Directory.GetFiles(folderPath);
                foreach (var file in files)
                {
                    await ProcessFile(file, virtualFolder);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error processing existing files in folder {folderPath}");
                _telemetryClient.TrackException(ex);
            }
        }


        private async Task ProcessFile(string filePath, string virtualFolder)
        {
            try
            {
                var fileName = Path.GetFileName(filePath);
                var fileDate = File.GetLastWriteTime(filePath);

                // Build the blob path using forward slashes
                var blobPath = $"{virtualFolder}/{fileDate:yyyy/MM/dd}/{fileName}";

                if (_configuration.GetValue<bool>("BlobStorage:UseHourlyOrganization"))
                {
                    blobPath = $"{virtualFolder}/{fileDate:yyyy/MM/dd/HH}/{fileName}";
                }

                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                // Upload to Azure Blob Storage
                var blobClient = _blobContainerClient.GetBlobClient(blobPath);

                if (!IsFileLocked(filePath))
                {
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
                    _telemetryClient.TrackEvent("FileUploaded", new Dictionary<string, string>
                    {
                        { "FileName", fileName },
                        { "BlobPath", blobPath }
                    });

                    if (virtualFolder == "trxdata")
                    {
                        File.Move(filePath, Path.Combine(_archiveFolder, Path.GetFileName(filePath)));
                    }
                    else 
                    {
                        File.Delete(filePath);
                    }

                    _processedFilesCount++;
                }

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

            _healthTimer?.Stop();
            _healthTimer?.Dispose();

            _trxWatcher?.Dispose();
            _imagesWatcher?.Dispose();

            return base.StopAsync(cancellationToken);
        }

        private bool IsFileLocked(string filePath)
        {
            try
            {
                using (FileStream stream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None))
                {
                    return false;
                }
            }
            catch (IOException)
            {
                return true;
            }
        }
    }
}
