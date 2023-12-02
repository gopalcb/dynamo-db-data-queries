"""
"""
import boto3
import botocore
import traceback


class CreateTable:
    def __init__(self, region) -> None:
        self.region = region
        self.dynamodb = boto3.client('dynamodb', region=region)


    def create_dynamodb_table(self, table_name):
        """
        create a user table with the following properties-
            first_name
            last_name
            email
            phone
            address
        """
        try:
            response = self.dynamodb.create_table(
                TableName=table_name,
                AttributeDefinitions=[
                    {
                    "AttributeName": "first_name",
                    "AttributeType": "S"
                    },
                    {
                    "AttributeName": "last_name",
                    "AttributeType": "S"
                    },
                    {
                    "AttributeName": "email",
                    "AttributeType": "S"
                    },
                    {
                    "AttributeName": "phone",
                    "AttributeType": "S"
                    },
                    {
                    "AttributeName": "address",
                    "AttributeType": "S"
                    }
                ],
                KeySchema=[
                    {
                    "AttributeName": "first_name",
                    "KeyType": "HASH"
                    },
                    {
                    "AttributeName": "last_name",
                    "KeyType": "HASH"
                    },
                    {
                    "AttributeName": "email",
                    "KeyType": "HASH"
                    },
                    {
                    "AttributeName": "phone",
                    "KeyType": "HASH"
                    },
                    {
                    "AttributeName": "address",
                    "KeyType": "HASH"
                    }
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1
                }
            )

            print(response)
            return response

        except botocore.exceptions.ClientError as e:
            print(f'Unexpected error: {e}')
            return None
        
        except Exception as e:
            trace = traceback.format_exc()
            print(trace)
