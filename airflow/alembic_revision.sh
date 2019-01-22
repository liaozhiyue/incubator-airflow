msg=$1
# use python home airflow models.py
alembic revision --autogenerate -m "${msg}"
