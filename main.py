from pyspark.sql import SparkSession , SQLContext
from pyspark import SparkContext, SparkConf
from FileSchemas import FileSchemas
from flask import Flask, request


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

    if schema_type == "departments":
        schema = FileSchemas().dept_schema()
    if schema_type == "jobs":
        schema = FileSchemas().job_schema()
    if schema_type == "employees":
        schema = FileSchemas().hired_schema()
    dataframe = spark.read.jdbc("jdbc:postgresql://localhost:5433/postgres", "employees", properties=properties)
    
    return f"{dataframe.count()}"

if __name__ == '__main__':
    app.run(port=8000)