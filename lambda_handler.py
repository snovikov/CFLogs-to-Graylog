import json
import urllib
import boto3
import gzip
import datetime
import time

print('Loading function')

# Fixed fields
# Please refer to https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html
LINE_FORMAT = {
    'date': 0,
    'time': 1,
    'edge_location': 2,
    'bytes_sent': 3,
    'client_ip': 4,
    'http_method': 5,
    #'host': 6,
    'query_path': 7,
    'status_code': 8,
    'referer': 9,
    'user_agent': 10,
    'query_string': 11,
    #'cookie': 12,
    'edge_result_type': 13,
    #'edge_request_id': 14,
    'server_name': 15,
    'protocol': 16,
    'bytes_received': 17,
    'response_time_seconds': 18,
    #'forwarded_for': 19,
    #'ssl_protocol': 20,
    #'ssl_cipher': 21,
    #'edge_response_result_type': 22,
    'protocol_version': 23,
    #'fle_status': 24,
    #'fle_encrypted_fields': 25,
}


#======================================================================================================================
# Auxiliary Functions
#======================================================================================================================
def parse_log(bucket_name, key_name):
    print '[parse_log] Start'

    num_requests = 0
    buffer_size = 1000
    retry_attempts = 10
    result = dict()
    try:
        #--------------------------------------------------------------------------------------------------------------
        print "[parse_log] \tDownload file from S3"
        #--------------------------------------------------------------------------------------------------------------
        filename = key_name.split('/')[-1]
        distribution_id = filename.split('.')[0]
        local_file_path = '/tmp/' + filename
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, key_name, local_file_path)

        logs = boto3.client('logs')
        stream = logs.describe_log_streams(logGroupName='CloudFront', logStreamNamePrefix=distribution_id)
        if not stream['logStreams']:
            logs.create_log_stream(logGroupName='CloudFront', logStreamName=distribution_id)
            stream = logs.describe_log_streams(logGroupName='CloudFront', logStreamNamePrefix=distribution_id)
        stream_tocken = stream['logStreams'][0].get('uploadSequenceToken', '0')

        #--------------------------------------------------------------------------------------------------------------
        print "[parse_log] \tRead content of file %s" % filename
        #--------------------------------------------------------------------------------------------------------------
        with gzip.open(local_file_path,'r') as content:
            for line in content:
                try:
                    if line.startswith('#'):
                        #TODO: parse Fields: comment for field names
                        continue

                    line_data = line.split('\t')
                    strtime = "%s %s" % (line_data[LINE_FORMAT['date']], line_data[LINE_FORMAT['time']])

                    timestamp = int(
                        time.mktime(
                            datetime.datetime.strptime(strtime, "%Y-%m-%d %H:%M:%S").timetuple()) * 1000)
                    if timestamp not in result:
                        result[timestamp] = list()
                    message = dict()
                    for name in LINE_FORMAT:
                        if name == 'date' or name == 'time':
                            continue
                        if name == 'response_time_seconds':
                            message['response_time_milliseconds'] = \
                                int(float(line_data[LINE_FORMAT[name]]) * 1000)
                        else:
                            message[name] = line_data[LINE_FORMAT[name]]
                    result[timestamp].append(message)
                    num_requests += 1

                    # Send messages every 'buffer_size' messages
                    if num_requests % buffer_size == 0:
                        print "[parse_log] \tSending log records %d - %s" % \
                              (num_requests - buffer_size + 1, num_requests)
                        for i in xrange(retry_attempts):
                            try:
                                stream_response = logs.put_log_events(
                                    logGroupName='CloudFront',
                                    logStreamName=distribution_id,
                                    logEvents=get_sorted_records(result),
                                    sequenceToken=stream_tocken)
                            except Exception:
                                print "[parse_log] \tInvalid sequence tocken. Try to renew."
                                stream = logs.describe_log_streams(logGroupName='CloudFront',
                                                                   logStreamNamePrefix=distribution_id)
                                stream_tocken = stream['logStreams'][0].get('uploadSequenceToken', '0')
                                continue
                            stream_tocken = stream_response.get('nextSequenceToken')
                            break
                        result = dict()
                        continue

                except Exception as e:
                    print ("[parse_log] \t\tError to process line: %s" % e)
                    return -1
            print "[parse_log] \tSending log records %d - %s" % \
                  (num_requests - num_requests % buffer_size + 1, num_requests)
            logs.put_log_events(
                logGroupName='CloudFront',
                logStreamName=distribution_id,
                logEvents=get_sorted_records(result),
                sequenceToken=stream_tocken)

    except Exception:
        print "[parse_log] \tError to read input file"

    print '[parse_log] End'
    return num_requests


def get_sorted_records(result):
    send_buffer = list()
    for key in sorted(result):
        for message in result[key]:
            send_buffer.append({'timestamp': key, 'message': json.dumps(message)})
    return send_buffer


#======================================================================================================================
# Lambda Entry Point
#======================================================================================================================
def lambda_handler(event, context):
    print '[lambda_handler] Start'
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key_name = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')

    try:
        #--------------------------------------------------------------------------------------------------------------
        print "[lambda_handler] \tReading input data and parse logs"
        #--------------------------------------------------------------------------------------------------------------
        num_requests = parse_log(bucket_name, key_name)

    except Exception as e:
        raise e
    print '[main] End'
    return num_requests

# Test Input
event = {
    'Records': [
        {
            's3': {
                'bucket': {
                    'name': '<BUCKETNAME>'
                },
                'object': {
                    'key': '<FILENAME>'
                }
            }
        }
    ]
}

if __name__ == '__main__':
    lambda_handler(event, None)