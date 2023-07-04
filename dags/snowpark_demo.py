"""
### Use the Snowpark Provider

This DAG demonstrates a simple machine learning pipeline using the Snowpark decorators 
and the Snowflake XCOM backend.  Rather than using a model registry the pipeline 
leverages the ability to pass complex data (including ML models) between tasks.


The Snowpark provider is in a dev status and not yet 
in the pypi registry. Instead the provider is available via a wheel file in the linked 
repository.
"""

from datetime import datetime
from airflow.decorators import dag, task, task_group
from astro import sql as aql
from astro.files import File 
from astro.sql.table import Table 
import os
from astronomer.providers.snowflake.utils.snowpark_helpers import SnowparkTable

demo_database = os.environ['DEMO_DATABASE']
demo_schema = os.environ['DEMO_SCHEMA']

@dag(dag_id='snowpark_demo', 
     default_args={
         "temp_data_output": "table",
         "temp_data_db": demo_database,
         "temp_data_schema": demo_schema,
         "temp_data_overwrite": True,
         "database": demo_database,
         "schema": demo_schema
         },
     schedule_interval=None, 
     start_date=datetime(2023, 4, 1))
def snowpark_provider_demo():

    _SNOWFLAKE_CONN_ID = 'snowflake_default'
    _SNOWPARK_BIN = '/home/astro/.venv/snowpark/bin/python'

    ingest_files=['yellow_tripdata_sample_2019_01.csv', 'yellow_tripdata_sample_2019_02.csv']
    raw_table = Table(name='TAXI_RAW', metadata={'database':demo_database, 'schema':demo_schema}, conn_id=_SNOWFLAKE_CONN_ID)

    @task_group()
    def load():

        for source in ingest_files:
            aql.load_file(task_id=f'load_{source}',
                input_file = File(f'include/data/{source}'), 
                output_table = raw_table,
                if_exists='replace'
            )
        
    @task.snowpark_python()
    def transform(raw_table:SnowparkTable) -> SnowparkTable:

        return raw_table.with_column('TRIP_DURATION_SEC',
                                     F.datediff('seconds', F.col('PICKUP_DATETIME'), F.col('DROPOFF_DATETIME')))\
                        .with_column('HOUR', F.date_part('hour', F.col('PICKUP_DATETIME').cast(T.TimestampType())))\
                        .select(F.col('PICKUP_LOCATION_ID').cast(T.StringType()).alias('PICKUP_LOCATION_ID'),
                                F.col('DROPOFF_LOCATION_ID').cast(T.StringType()).alias('DROPOFF_LOCATION_ID'),
                                F.col('HOUR'), 
                                F.col('TRIP_DISTANCE'), 
                                F.col('TRIP_DURATION_SEC'))

    @task.snowpark_virtualenv(python_version='3.9')
    def feature_engineering(taxidf:SnowparkTable) -> SnowparkTable:
        from sklearn.preprocessing import MaxAbsScaler
        import pandas as pd

        taxidf = taxidf.with_column('HOUR_OF_DAY', F.col('HOUR').cast(T.StringType())).to_pandas()

        cat_cols = pd.get_dummies(taxidf[['PICKUP_LOCATION_ID', 'DROPOFF_LOCATION_ID', 'HOUR_OF_DAY']])

        num_cols = pd.DataFrame(
                    MaxAbsScaler().fit_transform(taxidf[['TRIP_DISTANCE']]), 
                    columns=['TRIP_DISTANCE_SCALED']
                )

        taxidf = pd.concat([taxidf, cat_cols, num_cols], axis=1)
        
        return snowpark_session.create_dataframe(taxidf)

    @task.snowpark_ext_python(python=_SNOWPARK_BIN)
    def train(featuredf:SnowparkTable) -> bytes:
        from sklearn.linear_model import LinearRegression
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import mean_squared_error
        import pickle

        df = featuredf.to_pandas()
        X = df.drop(df[['PICKUP_LOCATION_ID', 'DROPOFF_LOCATION_ID', 'HOUR_OF_DAY', 'HOUR', 'TRIP_DURATION_SEC', 'TRIP_DISTANCE']], axis=1)
        y = df[['TRIP_DURATION_SEC']]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)
        
        lr = LinearRegression().fit(X_train, y_train)

        test_pred = lr.predict(X_test).reshape(-1)

        return pickle.dumps(lr)
        
    @task.snowpark_virtualenv(python_version='3.9')
    def predict(featuredf:SnowparkTable, model:bytes) -> SnowparkTable:
        import pickle

        pred_table_name = 'TAXI_PRED' 
        
        lr = pickle.loads(model)

        df = featuredf.to_pandas()
        X = df.drop(df[['PICKUP_LOCATION_ID', 'DROPOFF_LOCATION_ID', 'HOUR_OF_DAY', 'HOUR', 'TRIP_DURATION_SEC', 'TRIP_DISTANCE']], axis=1)
        
        df['PREDICTED_DURATION'] = lr.predict(X).astype(int)

        write_columns = ['PICKUP_LOCATION_ID', 'DROPOFF_LOCATION_ID', 'HOUR_OF_DAY', 'PREDICTED_DURATION', 'TRIP_DURATION_SEC']

        snowpark_session.write_pandas(
            df[write_columns], 
            table_name=pred_table_name,
            auto_create_table=True,
            overwrite=True
        )

        return SnowparkTable(name=pred_table_name)

    _rawdf = load() 

    _taxidf = transform(raw_table = raw_table)

    _featuredf = feature_engineering(_taxidf)

    _model = train(_featuredf)

    _pred = predict(_featuredf, _model)
    
    _rawdf >> _taxidf 

snowpark_provider_demo()