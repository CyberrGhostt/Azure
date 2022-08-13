import logging
import datetime
import json
from azure.eventhub import EventHubProducerClient, EventData
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://eventhubs-nspc.servicebus.windows.net/;SharedAccessKeyName=Policy;SharedAccessKey=NnfRZeB/mrskn/cO56mS/hBFaL0SsG/v0Kf5c/a40qI=;EntityPath=sensor-stream", eventhub_name="sensor-stream")
    event_data_batch = producer.create_batch()
    
    
    
    name = req.params.get('name')
    msg = req.params.get('msg')
    temperature = req.params.get('temperature')
    humadity = req.params.get('humadity')
    

    if not (name and msg and temperature and humadity):
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
            msg = req_body.get('msg')
            temperature = req_body.get('temperature')
            humadity = req_body.get('humadity')
        
    reading = {'name': name, 'timestamp': str(datetime.datetime.utcnow()), 'msg': msg, 'temperature': temperature, 'humidity': humadity}
    
    s = json.dumps(reading) # Convert the reading into a JSON string.
    event_data_batch.add(EventData(s)) # Add event data to the batch.
    print("Sent :", s) 
    producer.send_batch(event_data_batch)

    if name:
        return func.HttpResponse(f"Hello, {name}, {msg}, {temperature}, {humadity}. This HTTP triggered function executed successfully.",
             status_code=201)
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
    
