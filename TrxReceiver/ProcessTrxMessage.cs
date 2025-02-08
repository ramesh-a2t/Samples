using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace TrxReceiver
{
    public class ProcessTrxMessage
    {
        private readonly ILogger<ProcessTrxMessage> _logger;

        public ProcessTrxMessage(ILogger<ProcessTrxMessage> logger)
        {
            _logger = logger;
        }

        [Function(nameof(ProcessTrxMessage))]
        public void Run([ServiceBusTrigger("rsetrxtopic", "rsetrxocr", Connection = "ServiceBusConnection")] ServiceBusReceivedMessage message)
        {
            _logger.LogInformation("Message ID: {id}", message.MessageId);
            _logger.LogInformation("Message Body: {body}", message.Body);
            _logger.LogInformation("Message Content-Type: {contentType}", message.ContentType);
        }
    }

}
