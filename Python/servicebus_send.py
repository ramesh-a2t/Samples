import os
import asyncio
from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient
from azure.identity.aio import DefaultAzureCredential

FULLY_QUALIFIED_NAMESPACE = 'https://ubosimagetrxsb.servicebus.windows.net' # os.environ["SERVICEBUS_FULLY_QUALIFIED_NAMESPACE"]
QUEUE_NAME = 'imagetrxqueue' # os.environ["SERVICEBUS_QUEUE_NAME"]

async def send_single_message(sender):
    message = ServiceBusMessage("Single Message")
    await sender.send_messages(message)

async def send_a_list_of_messages(sender):
    messages = [ServiceBusMessage("Message in list") for _ in range(10)]
    await sender.send_messages(messages)

async def send_batch_message(sender):
    batch_message = await sender.create_message_batch()
    for _ in range(10):
        try:
            batch_message.add_message(ServiceBusMessage("Message inside a ServiceBusMessageBatch"))
        except ValueError:
            # ServiceBusMessageBatch object reaches max_size.
            # New ServiceBusMessageBatch object can be created here to send more data.
            break
    await sender.send_messages(batch_message)

async def main():
    credential = DefaultAzureCredential()
    try:
        servicebus_client = ServiceBusClient(FULLY_QUALIFIED_NAMESPACE, credential)
        async with servicebus_client:
            sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
            async with sender:
                await send_single_message(sender)
                #await send_a_list_of_messages(sender)
                #await send_batch_message(sender)
                
            await credential.close()
    except ValueError:
        print('Error')
        await credential.close()

    print("Send message is done.")

asyncio.run(main())
