from sqlalchemy import text


get_sql_server_data = text(
    """
query starts
query ends
"""
)


get_customer_data = """
query starts
-- Query Block to insert --
query ends
"""


def generate_insert_block(refresh_time_unit, refresh_time_value):
    if refresh_time_unit not in ["hour", "day"]:
        raise ValueError("Invalid refresh_time_unit")

    start_datetime = (
        f"current_timestamp - interval {refresh_time_value} {refresh_time_unit}"
    )
    end_datetime = "current_timestamp"

    return f"""
    INNER JOIN (
        (SELECT 
         FROM 
         WHERE time BETWEEN {start_datetime} AND {end_datetime}) table ON key = key
    """
