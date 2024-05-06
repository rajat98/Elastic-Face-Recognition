import base64
import json
import os
import subprocess
import time

# os.system('pip install boto3')
import boto3

SQS_REQUEST_QUEUE = "https://sqs.us-east-1.amazonaws.com/905418040722/1229565443-req-queue"
SQS_RESPONSE_QUEUE = "https://sqs.us-east-1.amazonaws.com/905418040722/1229565443-resp-queue"
AWS_REGION_NAME = "us-east-1"
AWS_ACCESS_KEY_ID = os.environ.get('DEV_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('DEV_SECRET_ACCESS_KEY')
S3_IN_BUCKET_NAME = "1229565443-in-bucket"
S3_OUT_BUCKET_NAME = "1229565443-out-bucket"
sqs = boto3.client('sqs', region_name=AWS_REGION_NAME,
                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3 = boto3.client('s3', region_name=AWS_REGION_NAME,
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
target_message_ids = []


def upload_to_s3(s3_bucket, s3_key, local_file_path):
    s3.upload_file(local_file_path, s3_bucket, s3_key)
    object_url = f"https://{s3_bucket}.s3.amazonaws.com/{s3_key}"
    print(f"File uploaded to S3: {object_url}")
    return object_url


def save_prediction_to_s3(s3_bucket, s3_key, text):
    s3.put_object(Body=text, Bucket=s3_bucket, Key=s3_key)
    object_url = f"https://{s3_bucket}.s3.amazonaws.com/{s3_key}"
    print(f"Text saved to S3: {object_url}")


def run_child_program(args):
    # result = subprocess.run(["python3", "face_recognition.py"] + args, capture_output=True, text=True,
    #                         cwd='./CSE546-Cloud-Computing/model/')
    result = subprocess.run(["/home/ec2-user/myenv/bin/python3", "face_recognition.py"] + args, capture_output=True,
                            text=True,
                            cwd='/home/ec2-user/model/')
    print(f"args: {args}")

    # result = os.system(f"cd /home/ec2-user/model && python3 /home/ec2-user/model/face_recognition.py {args[0]}")
    print(result)
    return result.stdout.strip()


def get_image_prediction(local_file_path):
    arguments_to_pass = [f"{local_file_path}"]
    result = run_child_program(arguments_to_pass)
    return result


def process_message(message):
    t1 = time.time()
    message_id = message['MessageId']
    message_body = json.loads(message['Body'])
    file_name = message_body["filename"]
    base64_encoded_image = message_body["base64_encoded"]
    decoded_bytes = base64.b64decode(base64_encoded_image)
    t2 = time.time()
    local_file_path = save_image_on_instance(decoded_bytes, file_name)
    t3 = time.time()
    input_image_s3_object_url = upload_to_s3(S3_IN_BUCKET_NAME, file_name, local_file_path)
    t4 = time.time()
    prediction_output = get_image_prediction(local_file_path)
    t5 = time.time()
    print(f"prediction: {prediction_output}")

    output_s3_object_url = save_prediction_to_s3(S3_OUT_BUCKET_NAME, get_file_name_without_extension(file_name),
                                                 prediction_output)
    t6 = time.time()
    push_prediction_to_sqs(message_id, file_name, prediction_output)
    t7 = time.time()
    print(f"process_message_time: {t5-t1} secs")
    print(f"image_decoding_time: {t2-t1} secs")
    print(f"local_image_write_time: {t3-t2} secs")
    print(f"s3_in_bucket_upload_time: {t4-t3} secs")
    print(f"image_prediction_time: {t5-t4} secs")
    print(f"s3_out_bucket_upload_time: {t6-t5} secs")
    print(f"sqs_push_time: {t7-t6} secs")
    print(f"Processing message: {message['Body']}")


def get_file_name_without_extension(filename):
    return filename.split(".")[0]


def push_prediction_to_sqs(message_id, file_name, prediction_output):
    image_data = {
        'message_id': message_id,
        'filename': file_name,
        'result': prediction_output
    }
    # Convert the dictionary to a JSON string
    message_body = json.dumps(image_data)
    print(message_body)
    response = sqs.send_message(
        QueueUrl=SQS_RESPONSE_QUEUE,
        MessageBody=message_body,
        DelaySeconds=0,
    )
    print(response)


def save_image_on_instance(decoded_bytes, file_name):
    output_file_path = "/home/ec2-user/" + file_name
    with open(output_file_path, 'wb') as output_file:
        output_file.write(decoded_bytes)
    return output_file_path


def delete_received_messages(messages):
    # Receive up to 10 messages from the queue (adjust MaxNumberOfMessages accordingly)
    #
    # messages = response.get('Messages', [])
    for message in messages:
        print(f"message:{message}")
        print(f"target_message_ids:{target_message_ids}")
        # Check if the current message's ID matches the target ID
        if 'Body' in message:
            body = json.loads(message['Body'])
            if body['message_id'] in target_message_ids:
                receipt_handle = message['ReceiptHandle']
                sqs.delete_message(
                    QueueUrl=SQS_RESPONSE_QUEUE,
                    ReceiptHandle=receipt_handle
                )
    response = sqs.receive_message(
        QueueUrl=SQS_RESPONSE_QUEUE,
        MaxNumberOfMessages=10,
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=20
    )
    messages = response.get('Messages', [])
    for message in messages:
        print(f"message:{message}")
        print(f"target_message_ids:{target_message_ids}")
        # Check if the current message's ID matches the target ID
        if 'Body' in message:
            body = json.loads(message['Body'])
            if body['message_id'] in target_message_ids:
                receipt_handle = message['ReceiptHandle']
                sqs.delete_message(
                    QueueUrl=SQS_RESPONSE_QUEUE,
                    ReceiptHandle=receipt_handle
                )


def main():
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_REQUEST_QUEUE,
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                MessageAttributeNames=['All'],
                VisibilityTimeout=60,
                WaitTimeSeconds=20  # Long polling
            )

            messages = response.get('Messages', [])
            for message in messages:
                process_message(message)

                # Delete received message from queue
                receipt_handle = message['ReceiptHandle']
                sqs.delete_message(
                    QueueUrl=SQS_REQUEST_QUEUE,
                    ReceiptHandle=receipt_handle
                )
                # delete_received_messages(messages)
            #     break
            # break
        except Exception as e:
            print(f"Error: {e}")

        # time.sleep(1)  # Add a delay to avoid high CPU usage during polling


if __name__ == "__main__":
    main()
