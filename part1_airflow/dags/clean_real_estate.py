# dags/real_est.py

import pendulum
from airflow.decorators import dag, task

@dag(
    dag_id='clean_real_est',
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["real_estate", "sprint_one", "mle_projects"]
)

def prepare_real_est_dataset():
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task()
    def create_table():
        import sqlalchemy
        from sqlalchemy import MetaData, Table, Column, Integer, BigInteger, Float, Boolean, UniqueConstraint

        hook = PostgresHook('destination_db')
        conn = hook.get_sqlalchemy_engine()

        metadata = MetaData()
        project_one_table = Table(
            'real_estate_clean',
            metadata,
            Column('id', BigInteger, primary_key=True),
            Column('flat_id', BigInteger),
            Column('building_id', Integer),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', Boolean),
            Column('studio', Boolean),
            Column('total_area', Float),
            Column('price', BigInteger),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            UniqueConstraint('flat_id', name='unique_f_id')
        )
        if not sqlalchemy.inspect(conn).has_table(project_one_table.name): 
            metadata.create_all(conn)

    @task()
    def extract(**kwargs):

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select * from real_estate
        """
        data = pd.read_sql(sql, conn).drop(columns=['id'])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        from steps.cleaning import remove_duplicates, get_ouliers

        # data = data.drop('studio', axis=1)
        remove_duplicates(data)
        outliers = get_ouliers(data)
        rows_to_remove = data[outliers]['flat_id'].tolist()
        data = data[~data['flat_id'].isin(rows_to_remove)]
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="real_estate_clean",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
    )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
prepare_real_est_dataset()
