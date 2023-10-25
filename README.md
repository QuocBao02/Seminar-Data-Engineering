TOPIC:Build data stored platform for Binance market data

![image](https://github.com/QuocBao02/Seminar-Data-Engineering/assets/136684847/13b8c74d-abe8-4d05-beb1-42c8c07f2dd3)
Steps:

Preparation: Install Hadoop, Spark and Hive on Ubuntu operating system.

Step 1: Collect data from the binance.com website using the API, then save all that data to Data Lake as raw data. This process will be performed using Python and Apache Spark (PySpark).

Step 2: ETL (Extract, Transform, Load): Use Apache Spark to extract data from Data Lake, perform necessary processing such as filtering, transforming and cleaning data, then save data to Data Lake. Warehouse via Hive. Data will be saved as a relational database (RDB) or reports that can be queried and processed using SQL through Hive.

Step 3: Visualize data by creating reports and charts based on data saved on Data Warehouse. Tools like Apache Superset, Tableau, or Power BI can be used to create compelling charts and reports to clearly see important information from the data.

Step 4: Use Apache Airflow to automate the entire process. Airflow will help determine the execution schedule of previous steps automatically, ensuring that data collection, ETL, and visualization are performed according to a predetermined schedule
