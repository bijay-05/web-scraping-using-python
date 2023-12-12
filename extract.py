import argparse
from configparser import ConfigParser
import pandas as pd
import psycopg2

def get_connection_object():
    parser = ConfigParser()
    parser.read("config.ini")

    db_keys = {}

    if "postgres" in parser.sections():
      params = parser.items("postgres")
      for param in params:
          db_keys[param[0]] = param[1]

    try:
        print("Let's see what happens with database connection")
        connection_to_db = psycopg2.connect(
          database=db_keys["database"],
          host=db_keys["host"],
          user=db_keys["user"],
          password=db_keys["password"],
          port=db_keys["port"]
        )

        return connection_to_db
    except Exception as ex:
        print("Unfortunately Database connection could not be formed.")
        print(ex)


def extract_table(database_connection: psycopg2.extensions.connection, table_name: str, filter=None : str, schema: list) -> str:
    """
    this function inputs a database connection object to manage database connection
    session required to extract data from table, and extracts the table from database,
    additional filter parameter can be used to define filter condition as string
    """

    with database_connection.cursor() as cursar:
        try:
            if filter == None:
                filename = f"{table_name}.csv"
                query = f"SELECT * FROM {table_name},"
                cursar.execute(query)
                result_list = cursar.fetchall()
                result_df = pd.DataFrame(result_list, columns=schema)
                result_df.to_csv(filename, index=False, header=True, encoding='utf-8')
                database_connection.close()
                return "ALL SUCCESSFUL $$$"
            else:
                query = f"SELECT * FROM {table_name} WHERE {filter};"
                cursar.execute(query)
                result_list = cursar.fetchall()
                result_df = pd.DataFrame(result_list, columns=schema)
                result_df.to_csv(filename, index=False, header=True, encoding='utf-8')
                database_connection.close()
                return "ALL SUCCESSFUL $$$"
        except Exception as eww:
            print(eww)
            return "UNSUCCESSFUL"


if __name__=='__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument(
      "--table_name",
      help="name of table to be extracted from database"
    )

    parser.add_argument(
      "--filter",
      default=None,
      help="condition to filter rows (in string format)"
    )

    schema_dict = {
      "products": ['product_id','product_name','unit_cost','product_category','product_subcategory','product_brand'],
      "warehouse": ['warehouse_id','address_state','address_city','address_street'],
      "supplier": ['supplier_id','supplier_name','address','contact'],
      "inventory": ['inventory_id','available_quantity','min_stock','max_stock','reorder_point','measure_unit','warehouse_id','product_id'],
      "adjustment": [' adjustment_id', 'warehouse_id', 'product_id', 'adjust_date', 'adjust_type','quantity_adjusted']
    }

    args = parser.parse_args()

    schema = schema_dict.get(args.table_name)

    connection_object = get_connection_object()

    status = extract_table(database_connection=connection_object, table_name=args.table_name, filter=args.filter, schema=schema)

    print(status)
