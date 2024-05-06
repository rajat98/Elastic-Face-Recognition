import time
import base64
import json

import boto3
from flask import Flask, request, make_response

app = Flask(__name__)
SQS_REQUEST_QUEUE = "https://sqs.us-east-1.amazonaws.com/905418040722/1229565443-req-queue"
SQS_RESPONSE_QUEUE = "https://sqs.us-east-1.amazonaws.com/905418040722/1229565443-resp-queue"
# Load environment variables from .zshrc file
AWS_ACCESS_KEY_ID = os.environ.get('DEV_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('DEV_SECRET_ACCESS_KEY')
AWS_REGION_NAME = 'us-east-1'
# logger = logging.getLogger()
# logger.info(AWS_REGION_NAME)
# logger.info(AWS_ACCESS_KEY_ID)
# logger.info(AWS_SECRET_ACCESS_KEY)
# print(AWS_REGION_NAME)
# print(AWS_ACCESS_KEY_ID)
# print(AWS_SECRET_ACCESS_KEY)
sqs = boto3.client('sqs', region_name=AWS_REGION_NAME,
                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

response_cache = dict()


async def fetch_response_from_queue(target_message_id):
    while True:
        if target_message_id in response_cache:
            return response_cache[target_message_id]
        # Receive up to 10 messages from the queue (adjust MaxNumberOfMessages accordingly)
        t1=time.time()
        response = sqs.receive_message(
            QueueUrl=SQS_RESPONSE_QUEUE,
            MaxNumberOfMessages=10,
            VisibilityTimeout=20,
            WaitTimeSeconds=1
        )

        messages = response.get('Messages', [])
        result = {}
        for message in messages:
            if 'Body' in message:
                body = json.loads(message['Body'])
                receipt_handle = message['ReceiptHandle']
                sqs.delete_message(
                    QueueUrl=SQS_RESPONSE_QUEUE,
                    ReceiptHandle=receipt_handle
                )
                key = body['message_id']
                value = json.loads(message['Body'])
                response_cache[key] = value
                if body['message_id'] == target_message_id and body['result'] != "":
                    result = json.loads(message['Body'])
                    return result
        t2=time.time()
        print(f"elapsed time:{t2-t1} seconds")
        #if result!={}:
        #   return result

async def process_request(request_data):
    # print(f"Sending request: filename {request_data[1:28]}")
    response = sqs.send_message(
        QueueUrl=SQS_REQUEST_QUEUE,
        MessageBody=request_data,
        DelaySeconds=0
    )
    # print(response)
    return response


async def handle_request(request_data):
    ack = await process_request(request_data)
    target_message_id = ack["MessageId"]
    response_result = await fetch_response_from_queue(target_message_id)
    print(response_result)

    result = get_response_text(response_result)
    response = make_response(result)
    response.headers['Content-Type'] = 'text/plain'
    return response


def get_response_text(response_result):
    if "result" not in response_result.keys() or "filename" not in response_result.keys():
        return "No response"
    file_name = response_result["filename"].rstrip(".jpg")
    result = response_result["result"]
    return f"{file_name}:{result}"


@app.route('/', methods=['POST'])
async def root():
    img = request.files.get('inputFile', None)
    file_name = img.filename
    encoded_image = base64.b64encode(img.read()).decode('utf-8')
    # Create a dictionary with image data
    image_data = {
        'filename': file_name,
        'base64_encoded': encoded_image
    }
    # Convert the dictionary to a JSON string
    message_body = json.dumps(image_data)
    response = await handle_request(message_body)
    return response


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)
