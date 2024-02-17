import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import sql_statements
import config  # Import the config file


def get_data_from_db(
    which_db, query_name, refresh_time_unit=None, refresh_time_value=None
):
    if which_db == "SQL Server":
        # Use SQL Server credentials from config.py
        server = config.SQL_SERVER_HOST
        database = config.SQL_SERVER_DATABASE
        username = config.SQL_SERVER_USERNAME
        password = config.SQL_SERVER_PASSWORD

        engine = create_engine(
            f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        )

        sql_query = getattr(sql_statements, query_name, None)

        with engine.connect() as connection:
            result_proxy = connection.execute(sql_query)
            rows = result_proxy.fetchall()
            column_names = result_proxy.keys()

        df = pd.DataFrame(rows, columns=column_names)

    elif which_db == "MySQL":
        # Use MySQL credentials from config.py
        sh_db = mysql.connector.connect(
            host=config.MYSQL_HOST,
            user=config.MYSQL_USER,
            passwd=config.MYSQL_PASSWORD,
            database=config.MYSQL_DATABASE,
            auth_plugin="mysql_native_password",
            port=config.MYSQL_PORT,
        )

        db_cursor = sh_db.cursor()
        full_query = getattr(sql_statements, query_name, None)

        if refresh_time_unit and refresh_time_value:
            place_holder = "-- add INNER JOIN block here --"
            inner_join_block = sql_statements.generate_inner_join_block(
                refresh_time_unit, refresh_time_value
            )
            full_query = full_query.replace(place_holder, inner_join_block)

        db_cursor.execute(full_query)
        rows = db_cursor.fetchall()
        column_names = [desc[0] for desc in db_cursor.description]

        df = pd.DataFrame(rows, columns=column_names)

        db_cursor.close()
        sh_db.close()

    return df


def enrich_data(df1, df2):
    # Convert the columns to the same data type before merging
    df1["key_df_1"] = df1["key_df_1"].astype(float)
    df2["key_df_2"] = df2["key_df_2"].astype(float)

    # Left join on key_df_1 and key_df_2
    df_combined = pd.merge(
        df1,
        df2,
        left_on="key_df_1",
        right_on="key_df_2",
        how="left",
    )

    # Create a new field new_field_1 with data from df1
    df_combined["new_field_1"] = df_combined["original_field_df1"]

    # Create a new field new_field_2 with data from df2
    df_combined["new_field_2"] = df_combined["original_field_df2"]

    # Create a new field new_field_3 with data from df2
    df_combined["new_field_3"] = df_combined["original_field_df2"]

    # Drop unnecessary columns
    columns_to_drop = [
        "original_field_df1",
        "original_field_df2",
        "key_df_1",
        "key_df_2",
    ]
    df_combined = df_combined.drop(columns=columns_to_drop)

    return df_combined


def get_full_data(refresh_time_unit=None, refresh_time_value=None):
    sql_server_Data = get_data_from_db(
        which_db="SQL Server", query_name="get_sql_server_data"
    )
    data_from_crm = get_data_from_db(
        which_db="MySQL",
        query_name="get_customer_data",
        refresh_time_unit=refresh_time_unit,
        refresh_time_value=refresh_time_value,
    )

    df = enrich_data(data_from_crm, sql_server_Data)
    return df
