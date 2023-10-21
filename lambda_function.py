import json
import boto3


firehose_client = boto3.client('firehose')

def lambda_handler(event, context):
    resultString = ""
    for record in event['Records']:
        parsedRecord = parseRawRecord(record['dynamodb'])
        resultString =  resultString  + json.dumps(parsedRecord) + "\n"
    print(resultString)
    response = firehose_client.put_record(
        DeliveryStreamName="OrdersAuditStream",
        Record={
            'Data': resultString
        }
)
    
def parseRawRecord(record):
    result = {}
    result["orderId"] = record['NewImage']['orderId']['S']
    result["state"] = record['NewImage']['state']['S']
    result["lastUpdatedAt"] = record['NewImage']['lastUpdatedAt']['N']
    return result
