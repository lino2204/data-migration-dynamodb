import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError
import time

# Sessions per account
src_session = boto3.Session(profile_name='')
dst_session = boto3.Session(profile_name='')

# Resources
src_table = src_session.resource('dynamodb', region_name='us-east-1').Table('')
dst_table = dst_session.resource('dynamodb', region_name='us-east-1').Table('')

# Serializer for writing to DynamoDB
serializer = TypeSerializer()

# Data migration function
def migrate_table():
    print("🚀 Starting migration...")
    total = 0
    start_key = None

    while True:
        scan_kwargs = {}
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key

        response = src_table.scan(**scan_kwargs)
        items = response.get('Items', [])

        with dst_table.batch_writer(overwrite_by_pkeys=['id']) as batch:
            for item in items:
                batch.put_item(Item=item)
                total += 1

        print(f"✅ Migrated {total} items so far...")

        start_key = response.get('LastEvaluatedKey')
        if not start_key:
            break

        # Sleep to avoid throttling
        time.sleep(0.2)

    print(f"🎉 Finished! Total items migrated: {total}")

# Run
if __name__ == '__main__':
    migrate_table()