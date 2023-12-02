import boto3
import botocore
import json
import uuid
import traceback


class DataStore:
    def __init__(self, region) -> None:
        self.region = region
        self.dynamodb = boto3.client('dynamodb', region=region)


    def upload_from_json_file(self, table_name):
        try:
            json_file_path = 'data.json'
            with open(json_file_path, 'r') as datafile:
                records = json.load(datafile)

            for record in records:
                print(record)
                item = {
                    'id': {'S': str(uuid.uuid4())},
                    'first_name':{'S':record['first_name']},
                    'last_name':{'S':record['last_name']},
                    'email':{'S': record['email']},
                    'phone':{'S': record['phone']},
                    'address':{'S': record['address']}
                }

                print(item)
                response = self.dynamodb.put_item(
                    TableName=table_name, 
                    Item=item
                )
                print('saving item')
                print(response)
                return response

        except botocore.exceptions.ClientError as e:
            print(f'Unexpected error: {e}')
            return None
        
        except Exception as e:
            print(traceback.format_exc())
            return None
        
    
    def save_data_items(self, table_name, records):
        """
        item = {
            'id': {'S': str(uuid.uuid4())},
            'first_name':{'S':record['first_name']},
            'last_name':{'S':record['last_name']},
            'email':{'S': record['email']},
            'phone':{'S': record['phone']},
            'address':{'S': record['address']}
        }
        """
        try:
            for record in records:
                print(record)
                item = {
                    'id': {'S': str(uuid.uuid4())},
                    'first_name':{'S':record['first_name']},
                    'last_name':{'S':record['last_name']},
                    'email':{'S': record['email']},
                    'phone':{'S': record['phone']},
                    'address':{'S': record['address']}
                }

                print(item)
                response = self.dynamodb.put_item(
                    TableName=table_name, 
                    Item=item
                )
                print('saving item')
                print(response)
                return response

        except botocore.exceptions.ClientError as e:
            print(f'Unexpected error: {e}')
            return None
        
        except Exception as e:
            print(traceback.format_exc())
            return None
