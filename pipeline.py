import os
import pandas as pd
import psycopg2
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

PG_TYPE_MAPPING = {
    "int64": "INTEGER",
    "float64": "DOUBLE PRECISION",
    "object": "TEXT",
}

class Pipeline:
    def __init__(self, database, user, password, host, port):
        try:
            self.conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
            )
            self.cur = self.conn.cursor()
        except Exception as e:
            print("Connection error:", e)

    def create_table_if_not_exists(self, table_name, df):
        column_defs = []
        for col, dtype in df.dtypes.items():
            pg_type = PG_TYPE_MAPPING.get(str(dtype), "TEXT")
            column_defs.append(f'"{col}" {pg_type}')
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join(column_defs)}
        );
        """
        self.cur.execute(create_table_query)
        self.conn.commit()

    def etl(self, data_path, table_name):
        df = pd.read_csv(data_path)

        self.create_table_if_not_exists(table_name, df)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        sql_query = f"""
           COPY {table_name} 
           FROM stdin 
           WITH CSV HEADER 
           DELIMITER as ',' 
        """

        self.cur.copy_expert(sql=sql_query, file=csv_buffer)

        self.conn.commit()
        self.cur.close()
        self.conn.close()


if __name__ == "__main__":
    pipeline = Pipeline(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=5432,
    )
    pipeline.etl(data_path="data/doordash_data.csv", table_name="doordash_data")
