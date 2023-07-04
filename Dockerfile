# syntax=quay.io/astronomer/airflow-extensions:latest

FROM quay.io/astronomer/astro-runtime:8.6.0-base

COPY include/astro_provider_snowflake-0.0.0-py3-none-any.whl /tmp

PYENV 3.9 snowpark requirements-snowpark.txt

#Seed the base 3.8 python with snowpark packages for virtualenv operator
COPY requirements-snowpark.txt /tmp
RUN python3.9 -m pip install -r /tmp/requirements-snowpark.txt