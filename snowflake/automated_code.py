from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType
from snowflake.snowpark import DataFrame
from typing import List, Dict, Any
import pandas as pd

def load_data(session: Session, table_name: str) -> DataFrame:
    """
    Load data from a Snowflake table into a Snowpark DataFrame.
    
    Args:
        session (Session): Snowflake session object.
        table_name (str): Name of the table to load data from.
        
    Returns:
        DataFrame: Snowpark DataFrame containing the loaded data.
    """
    return session.table(table_name)
