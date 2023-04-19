from pyspark.sql import SparkSession , SQLContext
from pyspark import SparkContext, SparkConf
from FileSchemas import FileSchemas
from flask import Flask, request
from pyspark.sql import functions as func

app = Flask(__name__)
spark = SparkSession.builder \
    .appName("PostgreSQL example") \
    .config("spark.driver.extraClassPath", "/home/facundo/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

properties = {
"user": "admin",
"password": "Passw0rd",
"driver": "org.postgresql.Driver"
} 
@app.route("/example",methods=["POST"])
def example_post():
    filename = request.form['filename']
    schema_type  = request.form["schema"]
    batch_size = request.form["batch"]
    if batch_size > 1000:
        return "invalid batch size, maximum 1000"
    if schema_type == "departments":
        schema = FileSchemas().dept_schema()
    elif schema_type == "jobs":
        schema = FileSchemas().job_schema()
    elif schema_type == "employees":
        schema = FileSchemas().hired_schema()
    else:
        return "invalid schema"

    dataframe = spark.read.schema(schema).csv(filename)
    def write_batch_to_db(df, id):
        df.write.jdbc(url="jdbc:postgresql://localhost:5433/postgres", table="employees", mode="append", properties=properties)

    
    dataframe.limit(batch_size).write.jdbc(url="jdbc:postgresql://localhost:5433/postgres", table="employees", mode="append", properties=properties)
    dataframe.exceptAll(
        dataframe.limit(batch_size)
        ).write.foreachBatch(
            write_batch_to_db
        ).option("batchSize", batch_size
        ).start()
    return 'INSERTED BATCHS'

if __name__ == '__main__':
    app.run(port=8000)