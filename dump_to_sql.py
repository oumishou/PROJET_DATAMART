import gc
import os
import sys
import io

import pandas as pd
from sqlalchemy import create_engine
from minio import Minio
from minio.error import S3Error

def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.DataFrame) : The dataframe to dump into the DBMS engine

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "oumou_warehouse",
        "dbms_table": "oumou_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success: bool = True
            print("Connection successful! Processing parquet file")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')

    except Exception as e:
        success: bool = False
        print(f"Error connecting to the database: {e}")
        return success

    return success

def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite it columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe

def main() -> None:
    # Configuration Minio
    minio_config = {
        "endpoint": "localhost:9000",
        "secure": False,
        "access_key": "minio",
        "secret_key": "minio123",
        "bucket": "oumishou"
    }

    try:
        minio_client = Minio(
            minio_config["endpoint"],
            access_key=minio_config["access_key"],
            secret_key=minio_config["secret_key"],
            secure=minio_config["secure"]
        )

        parquet_files = [obj.object_name for obj in minio_client.list_objects(minio_config["bucket"]) if obj.object_name.lower().endswith('.parquet')]

        for parquet_file in parquet_files:
            parquet_data = minio_client.get_object(minio_config["bucket"], parquet_file)
            parquet_content = parquet_data.read()
            parquet_df = pd.read_parquet(io.BytesIO(parquet_content), engine='pyarrow')

            clean_column_name(parquet_df)
            if not write_data_postgres(parquet_df):
                del parquet_df
                gc.collect()
                return

            del parquet_df
            gc.collect()

    except S3Error as e:
        print(f"Error accessing Minio: {e}")

if __name__ == '__main__':
    sys.exit(main())
