import boto3
import botocore
import traceback


class DataReader:
    def __init__(self, region) -> None:
        self.region = region
        self.dynamodb_client = boto3.client('dynamodb', region=region)
        self.dynamodb_resource = boto3.resource('dynamodb', region=region)


    def get_single_item_with_client(self, table_name, keys, values):
        """
        get single item from dynamo db table using boto3 client
        params:
            table_name: str
            keys: list
                example:    ['last_name', 'email']
                            ['id']
            values: list
                example:    ['bala', 'test@gmail.com']
                            ['xxx-xxx-xxx']
        return:
            response: dict
            sample response example:
                {
                    'id': {'S': 'dbea9bd8-fe1f-478a-a98a-5b46d481cf36'},
                    'first_name': {'S': 'First Name'},
                    'last_name': {'S': 'Last Name'},
                    'email': {'S': 'test@gmail.com'},
                    'phone': {'S': '111-111-1111'},
                    'address': {'S': 'test address'}
                }
        """
        try:
            """
            prepare key
            example:
                Key={
                    'last_name': {'S': 'Arturus Ardvarkian'},
                    'email': {'S': 'Carrot Eton'}
                }
            """
            key = {}
            for i, k in enumerate(keys):
                key[k] = values[i]

            response = self.dynamodb_client.get_item(
                TableName=table_name,
                Key=key
            )
            print(response['Item'])
            return response['Item']

        except botocore.exceptions.ClientError as e:
            print(f'Unexpected error: {e}')
            return None

        except Exception as e:
            print(traceback.format_exc())
            return None
        

    def get_single_item_with_resource(self, table_name, keys, values):
        """
        get single item from dynamo db table using boto3 resource
        params:
            table_name: str
            keys: list
                example:    ['last_name', 'email']
                            ['id']
            values: list
                example:    ['bala', 'test@gmail.com']
                            ['xxx-xxx-xxx']
        return:
            response: dict
            sample response example:
                {
                    'id': {'S': 'dbea9bd8-fe1f-478a-a98a-5b46d481cf36'},
                    'first_name': {'S': 'First Name'},
                    'last_name': {'S': 'Last Name'},
                    'email': {'S': 'test@gmail.com'},
                    'phone': {'S': '111-111-1111'},
                    'address': {'S': 'test address'}
                }
        """
        try:
            """
            prepare key
            example:
                Key={
                    'last_name': {'S': 'Arturus Ardvarkian'},
                    'email': {'S': 'Carrot Eton'}
                }
            """
            table = self.dynamodb_resource.Table(table_name)

            key = {}
            for i, k in enumerate(keys):
                key[k] = values[i]

            response = table.get_item(
                TableName=table_name,
                Key=key
            )
            print(response['Item'])
            return response['Item']

        except botocore.exceptions.ClientError as e:
            print(f'Unexpected error: {e}')
            return None

        except Exception as e:
            print(traceback.format_exc())
            return None
        

    def query_items_by_matching_partition_key_with_client(self, table_name, key, value):
        """
        use dynamo db client to query for items by matching a partition key
        params:
            table_name: str
            key: str
            value: str

        return:
            response: list(dict)
        """
        try:
            response = self.dynamodb_client.query(
                TableName=table_name,
                KeyConditionExpression=f'{key} = :{key}',
                ExpressionAttributeValues={
                    f':{key}': {'S': value}
                }
            )
            print(response['Items'])
            return response['Items']

        except botocore.exceptions.ClientError as e:
            print(f'Unexpected error: {e}')
            return None

        except Exception as e:
            print(traceback.format_exc())
            return None
        

    def query_items_by_matching_partition_key_with_resource(self, table_name, key, value):
        """
        use dynamo db resource to query for items by matching a partition key
        params:
            table_name: str
            key: str
            value: str

        return:
            response: list(dict)
        """
        try:
            table = self.dynamodb_resource.Table(table_name)
            response = table.query(
                KeyConditionExpression=Key(key).eq(value)
            )

            print(response['Items'])
            return response['Items']

        except botocore.exceptions.ClientError as e:
            print(f'Unexpected error: {e}')
            return None

        except Exception as e:
            print(traceback.format_exc())
            return None
        

    def query_items_using_partition_and_sort_keys_with_client(self, table_name, pkey, pvalue, skey):
        """
        use dynamo db client to query items by using partition and sort keys
        params:
            table_name: str
            pkey: str (partition key)
            pvalue: str
            skey: str (sort key)

        return:
            response: list(dict)
        """
        try:
            response = self.dynamodb_client.query(
                TableName=table_name,
                KeyConditionExpression=f'{pkey} = :{pkey} AND begins_with ( {skey} , :{skey} )',
                ExpressionAttributeValues={
                    f':{pkey}': {'S': pvalue},
                    f':{skey}': {'S': 'C'}
                }
            )
            print(response['Items'])
            return response['Items']

        except botocore.exceptions.ClientError as e:
            print(f'Unexpected error: {e}')
            return None

        except Exception as e:
            print(traceback.format_exc())
            return None