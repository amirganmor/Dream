import unittest
from unittest.mock import patch, MagicMock
import json
import pre_transform  # Import your pre_process module

class TestPreProcessLambda(unittest.TestCase):

    @patch('pre_transform.emr_client')  # Mock the EMR client
    @patch('pre_transform.config')  # Mock the config
    def test_lambda_handler_success(self, mock_config, mock_emr_client):
        # Mock configuration values
        mock_config.TRANSFORM_SCRIPT_PATH = 's3://your-script-path/script.py'
        mock_config.RAW_BUCKET_NAME = 'mock-raw-bucket'
        mock_config.OUTPUT_BUCKET = 'mock-output-bucket'
        mock_config.AGGREGATED_BUCKET = 'mock-aggregated-bucket'
        mock_config.EMR_JOB_FLOW_ID = 'j-XYZ123456789'  # Mock your EMR Job Flow ID
        
        # Mock EMR client response
        mock_response = {'StepIds': ['s-1234567890']}
        mock_emr_client.add_job_flow_steps.return_value = mock_response

        # Simulate incoming SQS event
        event = {
            'Records': [
                {'body': json.dumps({'file_name': 'raw_data_1.json'})},
                {'body': json.dumps({'file_name': 'raw_data_2.json'})},
                {'body': json.dumps({'file_name': 'raw_data_3.json'})}
            ]
        }
        
        # Invoke lambda handler
        context = {}  # Context can be left empty for this test
        response = pre_transform.lambda_handler(event, context)

        # Assertions to verify correct behavior
        self.assertEqual(response['statusCode'], 200)
        self.assertIn('Spark jobs submitted successfully!', response['body'])
        mock_emr_client.add_job_flow_steps.assert_called()  # Check if EMR steps were added

    @patch('pre_transform.emr_client')
    @patch('pre_transform.config')
    def test_lambda_handler_failure(self, mock_config, mock_emr_client):
        # Mock config
        mock_config.TRANSFORM_SCRIPT_PATH = 's3://your-script-path/script.py'
        mock_config.RAW_BUCKET_NAME = 'mock-raw-bucket'
        mock_config.OUTPUT_BUCKET = 'mock-output-bucket'
        mock_config.AGGREGATED_BUCKET = 'mock-aggregated-bucket'
        mock_config.EMR_JOB_FLOW_ID = 'j-XYZ123456789'

        # Simulate incoming SQS event
        event = {
            'Records': [
                {'body': json.dumps({'file_name': 'raw_data_1.json'})},
            ]
        }

        # Mock EMR client to raise an exception when submitting jobs
        mock_emr_client.add_job_flow_steps.side_effect = Exception("EMR job submission failed")
        
        # Invoke lambda handler
        context = {}
        response = pre_transform.lambda_handler(event, context)

        # Assertions to verify correct error handling
        self.assertEqual(response['statusCode'], 500)
        self.assertIn('Error: EMR job submission failed', response['body'])

if __name__ == '__main__':
    unittest.main()
