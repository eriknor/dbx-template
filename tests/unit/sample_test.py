from guardian_template_project.tasks.sample_etl_task import SQLTask
from guardian_template_project.tasks.sample_ml_task import SampleMLTask
from pyspark.sql import SparkSession
from pathlib import Path
import mlflow
import logging

def test_jobs(spark: SparkSession, tmp_path: Path):
    logging.info("Testing the ETL job")
    common_config = [
        # {
        #     "query":"create database if not exists nyctaxi;", 
        # }, 
        #     {
        #     "query":"create database if not exists test;", 
        #     },
            {
            "query":"create table if not exists trips as select * from (select 'jim' as name, 32 as age union select 'jill' as name, 21 as age);", 
            },
            {
            "filename":"example.sql", 
            "schema": "",
            "catalog":"",
            "source_catalog": "",
            "source_schema": ""},
            ]
    test_etl_config = {"queries": common_config}
    etl_job = SQLTask(spark, test_etl_config)
    etl_job.launch()
    table_name = f"example"
    _count = spark.table(table_name).count()
    assert _count > 0
    logging.info("Testing the ETL job - done")

    # logging.info("Testing the ML job")
    # test_ml_config = {
    #     "input": common_config,
    #     "experiment": "/Shared/guardian-template-project/sample_experiment"
    # }
    # ml_job = SampleMLTask(spark, test_ml_config)
    # ml_job.launch()
    # experiment = mlflow.get_experiment_by_name(test_ml_config['experiment'])
    # assert experiment is not None
    # runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
    # assert runs.empty is False
    # logging.info("Testing the ML job - done")


