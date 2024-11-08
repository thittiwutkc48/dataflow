from dotenv import load_dotenv
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions , WorkerOptions
from apache_beam import coders
from schema import GradeRevenuesRow
from helper import create_table, read_last_id_from_postgres, AddIncrementalId, partition_fn, execute_sql_query , connection_properties

load_dotenv()
coders.registry.register_coder(GradeRevenuesRow, coders.RowCoder)

# Access the environment variables
project = os.getenv('GOOGLE_CLOUD_PROJECT')
region = os.getenv('GOOGLE_CLOUD_REGION')
temp_location = os.getenv('GOOGLE_CLOUD_TEMP_LOCATION')
runner = os.getenv('GOOGLE_CLOUD_RUNNER')
machine_type = os.getenv('WORKER_MACHINE_TYPE')
num_workers = int(os.getenv('WORKER_NUM_WORKERS'))


def run():
 
    options = PipelineOptions()

    # Set Google Cloud options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project
    google_cloud_options.region = region
    google_cloud_options.job_name = 'bq-to-postgres-revenues'
    google_cloud_options.temp_location = temp_location

    # Set the runner (e.g., DataflowRunner or DirectRunner)
    options.view_as(StandardOptions).runner = 'DirectRunner'

    # Configure worker options
    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type = 'n2-standard-8'  # Adjust based on your requirements
    worker_options.num_workers = 4  # Adjust based on your requirements

    dataset = "slp_grading_dm"
    bq_table = "grade_revenues"
    schema_name="grade"
    last_id = read_last_id_from_postgres()

    with beam.Pipeline(options=google_cloud_options) as pipeline:
        from apache_beam.io.jdbc import WriteToJdbc
        from schema import GradeRevenuesRow,dict_to_grade_revenues_row , convert_to_utc_timestamp

        rows = (
            pipeline
                | "ReadFromBigQuery" >> beam.io.ReadFromBigQuery(
                    query=f'SELECT * FROM {dataset}.{bq_table} ',
                    use_standard_sql=True
                )
                | "AddIncrementalId" >> beam.ParDo(AddIncrementalId(last_id))  
                | f"ConvertToGradeRevenuesRow" >> beam.Map(lambda row: dict_to_grade_revenues_row(row, GradeRevenuesRow)).with_output_types(GradeRevenuesRow)
                | f"WriteToPosgresql" >> WriteToJdbc(
                            table_name=f'{schema_name}.{bq_table}_temp',
                            jdbc_url=connection_properties['jdbc_url'],
                            driver_class_name = "org.postgresql.Driver",
                            username=connection_properties['user'],
                            password=connection_properties['password']
                            )
            )


if __name__ == '__main__':
    create_table()
    run()
    execute_sql_query()
