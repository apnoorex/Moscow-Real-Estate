# dags/real_est.py

import pendulum
from airflow.decorators import dag, task
from steps.messages import send_telegram_success_message, send_telegram_failure_message

@dag(
    dag_id='real_estate',
    schedule='@once',
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
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
            'real_estate',
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
            UniqueConstraint('flat_id', name='unique_flat_id')
        )
        if not sqlalchemy.inspect(conn).has_table(project_one_table.name): 
            metadata.create_all(conn)

    @task()
    def extract(**kwargs):

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
            SELECT f.id AS flat_id, f.building_id, f.floor, f.kitchen_area, f.living_area,
                f.rooms, f.is_apartment, f.studio, f.total_area, f.price,
                b.build_year, b.building_type_int, b.latitude, b.longitude, b.ceiling_height,
                b.flats_count, b.floors_total, b.has_elevator
            FROM buildings AS b
            LEFT JOIN flats AS f ON b.id = f.building_id 
            """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        data['studio'].replace({False: None}, inplace=True)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="real_estate",
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
