import unittest
from unittest.mock import patch, MagicMock
import extract
import json

class TestExtractLambda(unittest.TestCase):

    @patch('extract.s3')  # Mock the S3 client
    @patch('extract.sqs')  # Mock the SQS client
    @patch('requests.get')  # Mock the requests.get call
    @patch('extract.config')  # Mock the config
    def test_lambda_handler_success(self, mock_config, mock_requests_get, mock_sqs, mock_s3):
        # Mock configuration values
        mock_config.API_URL = 'https://mockapi.com/data'
        mock_config.BUCKET_NAME = 'mock-bucket'
        mock_config.SQS_QUEUE_URL = 'https://sqs.mock.queue/url'
        mock_config.MAX_CHUNK_SIZE = 1000

        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'id': 1, 'name': 'Test'}] * 100  # Mock API returning 100 items
        mock_requests_get.return_value = mock_response

        # Mock S3 and SQS interactions
        mock_s3.put_object.return_value = None
        mock_sqs.send_message.return_value = None

        # Invoke the lambda handler
        event = {}
        context = {}  # Context can be left empty for this test
        extract.lambda_handler(event, context)

        # Assertions to verify correct behavior
        mock_requests_get.assert_called_once_with('https://mockapi.com/data', timeout=10)
        mock_s3.put_object.assert_called()  # Check if S3 put_object was called
        mock_sqs.send_message.assert_called()  # Check if SQS send_message was called

    @patch('extract.s3')
    @patch('extract.sqs')
    @patch('requests.get', side_effect=Exception("API request failed"))
    @patch('extract.config')
    def test_lambda_handler_api_failure(self, mock_config, mock_requests_get, mock_sqs, mock_s3):
        # Mock config
        mock_config.API_URL = 'https://mockapi.com/data'
        
        # Invoke lambda handler, simulating API failure
        event = {}
        context = {}
        extract.lambda_handler(event, context)

        # Ensure S3 and SQS interactions never happened due to API failure
        mock_s3.put_object.assert_not_called()
        mock_sqs.send_message.assert_not_called()

    @patch('extract.s3')
    @patch('extract.config')
    def test_save_data_to_s3(self, mock_config, mock_s3):
        # Mock config
        mock_config.BUCKET_NAME = 'mock-bucket'
        
        # Call the _save_data_to_s3 function directly
        data = [{'id': 1, 'name': 'Test'}]
        extract._save_data_to_s3(data)
        
        # Verify that the S3 put_object method was called correctly
        mock_s3.put_object.assert_called_once()
    
    @patch('extract.sqs')
    @patch('extract.config')
    def test_send_sqs_message(self, mock_config, mock_sqs):
        # Mock config
        mock_config.SQS_QUEUE_URL = 'https://sqs.mock.queue/url'
        
        # Call the _send_sqs_message function directly
        file_name = 'raw_data.json'
        extract._send_sqs_message(file_name)
        
        # Verify that the SQS send_message method was called correctly
        mock_sqs.send_message.assert_called_once_with(
            QueueUrl='https://sqs.mock.queue/url',
            MessageBody=json.dumps({"file_name": file_name})
        )

if __name__ == '__main__':
    unittest.main()
