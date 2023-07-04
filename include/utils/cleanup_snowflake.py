from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import argparse

def cleanup(snowflake_conn_id:str, database:str, schema:str):
    
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    tables = ['_SYSTEM_REGISTRY_METADATA', 
              '_SYSTEM_REGISRTRY_DEPLOYMENTS', 
              '_SYSTEM_REGISTRY_MODELS',
              'XCOM_TABLE']
    views = ['_SYSTEM_REGISRTRY_DEPLOYMENTS_VIEW', 
             '_SYSTEM_REGISTRY_METADATA_LAST_DESCRIPTION', 
             '_SYSTEM_REGISTRY_METADATA_LAST_METRICS',
             '_SYSTEM_REGISTRY_METADATA_LAST_REGISTRATION',
             '_SYSTEM_REGISTRY_METADATA_LAST_TAGS',
             '_SYSTEM_REGISTRY_MODELS_VIEW'
             ]
    stages = ['xcom_stage']

    print(f"WARNING: DANGER ZONE!  This will drop the \ntables {tables}, \nviews {views} \nand stages {stages} in schema {database}.{schema}")
    prompt = input("Are you sure you want to do this?: N/y: ")

    if prompt.upper() == 'Y':
        prompt = input("Are you REALLY sure?: N/y: ")

        if prompt.upper() == 'Y':

            for table in tables:
                hook.run(f"DROP TABLE IF EXISTS {table};")
            
            for view in views:
                hook.run(f"DROP VIEW IF EXISTS {view};")

            for stage in stages:
                hook.run(f"DROP STAGE IF EXISTS {stage};")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Cleanup Snowflake tables and views craeted in the demo.',
        allow_abbrev=False)

    parser.add_argument('--conn_id', 
                        dest='snowflake_conn_id', 
                        help="Airflow connection name. Default: 'snowflake_default'",
                        default='snowflake_default')
    parser.add_argument('--database', 
                        dest='database',
                        help="Database name to create. Default: 'demo'")
    parser.add_argument('--schema', 
                        dest='schema',
                        help="Schema name to create for the demo data. Default: 'demo'")

    args = parser.parse_args()

    cleanup(snowflake_conn_id=args.snowflake_conn_id, 
            database=args.database,
            schema=args.schema)