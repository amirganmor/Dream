import unittest
from unittest.mock import patch, MagicMock
from airflow import DAG
from airflow.utils.dag_cycle_tester import test_cycle
from airflow.utils.state import State
from airflow.models import DagBag
from datetime import datetime

class TestEnhancedETLDAG(unittest.TestCase):

    @patch('airflow.providers.amazon.aws.operators.lambda_function.AWSLambdaInvokeFunctionOperator.execute')
    @patch('airflow.providers.amazon.aws.sensors.s3_key.S3KeySensor.poke')
    def test_etl_dag_success(self, mock_poke, mock_invoke_lambda):
        # Mock the behavior of the Lambda invocations
        mock_invoke_lambda.return_value = 'Success'

        # Mock the S3KeySensor to always return True, simulating the presence of files
        mock_poke.return_value = True

        # Load the DAG
        dagbag = DagBag()
        dag = dagbag.get_dag('enhanced_etl_pipeline_with_lambda')

        # Ensure there are no cycles in the DAG
        test_cycle(dag)

        # Set the execution date to simulate a run
        execution_date = datetime(2024, 10, 18)
        
        # Run the DAG
        dag.clear()
        dag.run(start_date=execution_date, end_date=execution_date, execute=True)

        # Verify that all tasks were executed successfully
        for task in dag.tasks:
            self.assertEqual(task.state, State.SUCCESS)

    @patch('airflow.providers.amazon.aws.operators.lambda_function.AWSLambdaInvokeFunctionOperator.execute')
    @patch('airflow.providers.amazon.aws.sensors.s3_key.S3KeySensor.poke')
    def test_etl_dag_failure(self, mock_poke, mock_invoke_lambda):
        # Mock the behavior of the Lambda invocations to simulate a failure
        mock_invoke_lambda.side_effect = Exception("Lambda function failed")

        # Mock the S3KeySensor to return True for the first task, simulating file presence
        mock_poke.return_value = True

        # Load the DAG
        dagbag = DagBag()
        dag = dagbag.get_dag('enhanced_etl_pipeline_with_lambda')

        # Set the execution date to simulate a run
        execution_date = datetime(2024, 10, 18)

        # Run the DAG
        dag.clear()
        dag.run(start_date=execution_date, end_date=execution_date, execute=True)

        # Verify that the first task ran and failed
        self.assertEqual(dag.get_task('invoke_lambda').state, State.FAILED)
        # Check that subsequent tasks did not run
        for task in dag.tasks[1:]:  # skip the first task
            self.assertEqual(task.state, State.NONE)

if __name__ == '__main__':
    unittest.main()
