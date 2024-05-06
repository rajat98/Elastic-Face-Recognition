import time
from time import sleep

import boto3

# Load environment variables from .zshrc file
AWS_ACCESS_KEY_ID = os.environ.get('DEV_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('DEV_SECRET_ACCESS_KEY')

AWS_REGION_NAME = 'us-east-1'
REQUEST_PROCESSING_TIME_IN_SECONDS = 1
DOWNSCALE_THRESHOLD = 5
downscale_request_count = 0
# global instance_id
APP_TIER_AMI = 'ami-0936f91d343954ac4'
current_instance_count = 0

SQS_REQUEST_QUEUE = "https://sqs.us-east-1.amazonaws.com/905418040722/1229565443-req-queue"
SQS_RESPONSE_QUEUE = "https://sqs.us-east-1.amazonaws.com/905418040722/1229565443-resp-queue"

MIN_INSTANCE_COUNT = 0
MAX_INSTANCE_COUNT = 20

sqs = boto3.client('sqs', region_name=AWS_REGION_NAME,
                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def get_ec2_resource():
    return boto3.resource(
        'ec2',
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def get_ec2_client():
    return boto3.client('ec2', region_name=AWS_REGION_NAME,
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def launch_ec2_instance():
    security_group_ids = ['sg-0c8817df00a236fe9']
    ec2_resource = get_ec2_resource()
    _, key_pair_name = get_or_create_key_pair(ec2_resource)
    instance = ec2_resource.create_instances(
        ImageId=APP_TIER_AMI,
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        KeyName=key_pair_name,
        TagSpecifications=[{'ResourceType': 'instance',
                            'Tags': [{
                                'Key': 'Name',
                                'Value': f'app-tier-instance-{current_instance_count}'}]}],
        NetworkInterfaces=[{'AssociatePublicIpAddress': True,
                            'DeviceIndex': 0,
                            'Groups': security_group_ids}],

    )

    instance_id = instance[0].id
    print(f"EC2 Instance launched with ID: {instance_id} at {time.asctime()}")


def get_or_create_key_pair(ec2_resource):
    key_pair_name = 'id_rsa'
    ec2_client = get_ec2_client()
    response = ec2_client.describe_key_pairs()
    key_pair_exists = len(response['KeyPairs']) > 0
    if not key_pair_exists:
        response = ec2_resource.create_key_pair(KeyName=key_pair_name)
    return response, key_pair_name


def autoscaler():
    request_queue_stats = sqs.get_queue_attributes(
        QueueUrl=SQS_REQUEST_QUEUE,
        AttributeNames=[
            'ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible',
            'ApproximateNumberOfMessagesDelayed']
    )
    response_queue_stats = sqs.get_queue_attributes(
        QueueUrl=SQS_RESPONSE_QUEUE,
        AttributeNames=[
            'ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible',
            'ApproximateNumberOfMessagesDelayed']
    )
    recalibrate_app_tier_instance_count(request_queue_stats['Attributes'], response_queue_stats['Attributes'])


def recalibrate_app_tier_instance_count(request_queue_stats, response_queue_stats):
    global downscale_request_count
    approximate_number_of_messages_in_request_queue = int(request_queue_stats['ApproximateNumberOfMessages'])
    approximate_number_of_messages_in_flight_in_request_queue = int(
        request_queue_stats['ApproximateNumberOfMessagesNotVisible'])
    approximate_number_of_messages_in_response_queue = int(response_queue_stats['ApproximateNumberOfMessages'])
    approximate_number_of_messages_in_flight_in_response_queue = int(
        response_queue_stats['ApproximateNumberOfMessagesNotVisible'])
    print(f"Approximate number of message in req queue: {approximate_number_of_messages_in_request_queue}")
    print(
        f"Approximate number of message in flight in req queue: {approximate_number_of_messages_in_flight_in_request_queue}")
    print(f"Approximate number of message in res queue: {approximate_number_of_messages_in_response_queue}")
    print(
        f"Approximate number of message in flight in res queue: {approximate_number_of_messages_in_flight_in_response_queue}")
    total_number_of_messages_in_queue = (approximate_number_of_messages_in_request_queue +
                                         approximate_number_of_messages_in_flight_in_request_queue +
                                         approximate_number_of_messages_in_response_queue +
                                         approximate_number_of_messages_in_flight_in_response_queue)
    print(f"current instance count: {current_instance_count}")
    # Request queue is empty, downscale aggressively
    if total_number_of_messages_in_queue == 0 and current_instance_count != 0:
        if downscale_request_count < DOWNSCALE_THRESHOLD:
            downscale_request_count +=1
        else:
            downscale_request_count = 0
            print("Option 0")
            print(f"Triggered downscale at {time.asctime()}")
            # sleep(60)
            downscale_app_tier(current_instance_count)
    elif approximate_number_of_messages_in_request_queue > current_instance_count and current_instance_count < MAX_INSTANCE_COUNT:
        print(f"Triggered upscale at {time.asctime()}")
        print("Option 2")
        downscale_request_count = 0
        instance_required = approximate_number_of_messages_in_request_queue - current_instance_count
        instance_required_with_limit = MAX_INSTANCE_COUNT - current_instance_count
        upscale_instance_count = min(instance_required, instance_required_with_limit)
        upscale_app_tier(upscale_instance_count)
    # Num of messages == Num of Instance
    else:
        downscale_request_count = 0
        print("Option 3")
        pass


def terminate_instance(instance_id):
    ec2_client = get_ec2_client()
    response = ec2_client.terminate_instances(
        InstanceIds=[
            instance_id,
        ],
    )

    print(response)
    print(F"Terminated at {time.asctime()}")


def upscale_app_tier(freq):
    global current_instance_count

    for i in range(freq):
        current_instance_count += 1
        launch_ec2_instance()


def downscale_app_tier(freq):
    global current_instance_count

    ec2_client = get_ec2_client()
    response = ec2_client.describe_instances(Filters=[
        {'Name': 'tag-key', 'Values': ['Name']},
        {'Name': 'tag-value', 'Values': ['app-tier-instance-*']},
        {'Name': 'instance-state-name', 'Values': ['running']}
    ])
    print(response)
    terminated_instance = 0
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instance_id = instance['InstanceId']
            terminate_instance(instance_id)
            terminated_instance += 1
            current_instance_count -= 1
            if terminated_instance == freq:
                return


if __name__ == '__main__':
    while True:
        # print(f"Running autoscaler in inf loop: {time.asctime()}")
        autoscaler()
        sleep(REQUEST_PROCESSING_TIME_IN_SECONDS)
