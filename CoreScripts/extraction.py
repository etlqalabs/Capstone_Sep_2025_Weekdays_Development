import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle


# database connection
from CommonUtilities.utilities import sales_data_from_Linux_server
from Configuration.etlconfig import *

oracle_engine = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")

mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")


class DataExtraction:
    # 1. extract the sales data from Linux servers and load in to mysql staging table
    def extract_sales_data_and_load_stage(self):
        sales_data_from_Linux_server(self)
        df = pd.read_csv("SourceSystem/sales_data_linux.csv")
        df.to_sql("staging_sales",mysql_engine,index=False)

    # 2. extract the product data from local file servers and load in to mysql staging table
    def extract_product_data_and_load_stage(self):
        df = pd.read_csv("SourceSystem/product_data.csv")
        df.to_sql("staging_product",mysql_engine,index=False)


    # 3. extract the inventory data from local file servers and load in to mysql staging table
    def extract_inventory_data_and_load_stage(self):
        df = pd.read_xml("SourceSystem/inventory_data.xml",xpath=".//item")
        df.to_sql("staging_inventory", mysql_engine, index=False)

    # 4. extract the supplier data from local file servers and load in to mysql staging table
    def extract_supplier_data_and_load_stage(self):
        df = pd.read_json("SourceSystem/supplier_data.json")
        df.to_sql("staging_supplier", mysql_engine, index=False)

    # 5. extract the stores data from orcale table  and load in to mysql staging table
    def extract_stores_data_and_load_stage(self):
        query = """select * from stores"""
        df = pd.read_sql(query,oracle_engine)
        df.to_sql("staging_stores", mysql_engine, index=False)



if __name__ == "__main__":
    de = DataExtraction()
    de.extract_sales_data_and_load_stage()
    de.extract_product_data_and_load_stage()
    de.extract_inventory_data_and_load_stage()
    de.extract_supplier_data_and_load_stage()
    de.extract_stores_data_and_load_stage()


