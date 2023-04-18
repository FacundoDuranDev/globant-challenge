from pyspark.sql import SparkSession
from pyspark import SparkContext
from FileSchemas import FileSchemas
from flask import Flask, request


app = Flask(__name__)

@app.route("/example",methods=["POST"])
def example_post():
    filename = request.form['filename']
    schema = request.form["schema"]

    if schema == "departaments":
        schema = FileSchemas().dept_schema
    if schema == "jobs":
        schema = FileSchemas().job_schema
    if schema == "hired":
        schema = FileSchemas().hired_schema
    spark = SparkContext("local")

    dataframe = spark.read.csv(filename).schema(schema)
    dataframe.show()
    return dataframe.count()



    
if __name__ == '__main__':
    app.run(port=8000)