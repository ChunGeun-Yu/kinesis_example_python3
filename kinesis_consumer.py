# -*- coding: utf-8 -*-

import boto3
import time
import json

client = boto3.client('kinesis', region_name='ap-northeast-2', aws_access_key_id='your_key', aws_secret_access_key='your_secret')

def get_records():

    response = client.describe_stream(StreamName='data_stream_test')

    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    # shard가 여러개이거나 reshard되어 일시적으로 여러개인 경우 Shards 갯수만큼 for loop 해야함.

    shard_iterator = client.get_shard_iterator(StreamName='data_stream_test',
                                                        ShardId=my_shard_id,
                                                        ShardIteratorType='LATEST')

    my_shard_iterator = shard_iterator['ShardIterator']

    record_response = client.get_records(ShardIterator=my_shard_iterator,
                                                Limit=10)

    while 'NextShardIterator' in record_response:
        record_response = client.get_records(ShardIterator=record_response['NextShardIterator'],
                                                    Limit=10)

        print('record_response: {}'.format(record_response))

        records = record_response['Records']
        if len(records) > 0:
            for x in records:
                print('data: {}'.format(x['Data']))
            print('===============')

        # wait for 5 seconds
        time.sleep(5)


def main():
    get_records()


if __name__ == "__main__":
    # execute only if run as a script
    main()