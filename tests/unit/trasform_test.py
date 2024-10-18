import unittest
from unittest.mock import patch, MagicMock
from loading import (
    lambda_handler,
    process_message,
    parse_s3_event,
    load_data_to_snowflake,
)

class TestLoading(unittest.TestCase):

    @patch('loading.sqs.receive_message')
    @patch('loading.sqs.delete_message')
    @patch('loading.process_message')
    def test_lambda_handler_success(self, mock_process_message, mock_delete_message, mock_receive_message):
        # Arrange
        mock_receive_message.return_value = {
            'Messages': [
                {'Body': '{"Records": [{"s3": {"bucket": {"name": "mock-bucket"}, "object": {"key": "mock-key"}}}]}'}  # Mock S3 event
            ]
        }
        
        mock_process_message.return_value = None
        
        # Act
        response = lambda_handler({}, {})
        
        # Assert
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(response['body'], 'Processing complete.')
        mock_receive_message.assert_called_once()
        mock_process_message.assert_called_once()

    @patch('loading.snowflake.connector.connect')
    @patch('loading.sqs.delete_message')
    @patch('loading.parse_s3_event')
    @patch('loading.load_data_to_snowflake')
    def test_process_message_success(self, mock_load_data_to_snowflake, mock_parse_s3_event, mock_delete_message, mock_snowflake_connect):
        # Arrange
        message = {
            'Body': '{"Records": [{"s3": {"bucket": {"name": "mock-bucket"}, "object": {"key": "transformed_data/mock-key"}}}]}',
            'ReceiptHandle': 'mock-receipt-handle'
        }
        mock_parse_s3_event.return_value = {'bucket': 'mock-bucket', 'key': 'transformed_data/mock-key'}
        mock_snowflake_connection = MagicMock()
        mock_snowflake_connect.return_value = mock_snowflake_connection
        
        # Act
        process_message(message)
        
        # Assert
        mock_parse_s3_event.assert_called_once_with(message['Body'])
        mock_load_data_to_snowflake.assert_called_once_with(mock_snowflake_connection, 'mock-bucket', 'transformed_data/mock-key', 'FINAL_TRANSFORMED_TABLE')
        mock_delete_message.assert_called_once_with(QueueUrl='mock-sqs-url', ReceiptHandle='mock-receipt-handle')

    def test_parse_s3_event_success(self):
        # Arrange
        event_body = '{"Records": [{"s3": {"bucket": {"name": "mock-bucket"}, "object": {"key": "mock-key"}}}]}'
        
        # Act
        result = parse_s3_event(event_body)
        
        # Assert
        expected = {'bucket': 'mock-bucket', 'key': 'mock-key'}
        self.assertEqual(result, expected)

    @patch('loading.snowflake.connector.connect')
    def test_load_data_to_snowflake_success(self, mock_connect):
        # Arrange
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        bucket = 'mock-bucket'
        key = 'mock-key'
        table_name = 'FINAL_TRANSFORMED_TABLE'
        
        # Act
        load_data_to_snowflake(mock_connection, bucket, key, table_name)
        
        # Assert
        expected_copy_sql = f"""
        COPY INTO {table_name}
        FROM 's3://{bucket}/{key}'
        FILE_FORMAT = (TYPE = 'PARQUET')
        ON_ERROR = 'CONTINUE';
        """
        mock_connection.cursor.return_value.__enter__.return_value.execute.assert_any_call(expected_copy_sql)
        mock_connection.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
