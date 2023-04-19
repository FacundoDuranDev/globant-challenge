from pyspark.sql import SparkSession , SQLContext
from pyspark import SparkContext, SparkConf
from FileSchemas import FileSchemas
from flask import Flask, request
from pyspark.sql import functions as func

app = Flask(__name__)
spark = SparkSession.builder.appName(
        "PostgreSQL example"
    ).config(
        "spark.driver.extraClassPath",
        "/home/facundo/jars/postgresql-42.6.0.jar"
    ).getOrCreate()

properties = {
"user": "admin",
"password": "Passw0rd",
"driver": "org.postgresql.Driver"
} 
@app.route("/example",methods=["POST"])
def example_post():
    filename = request.form['filename']
    schema_type  = request.form["schema"]
    try:
        batch_size = int(request.form["batch"])
    except ValueError:
        return "invalid batch size, batch size needs to be a number"
    if batch_size > 1000:
        return "invalid batch size, maximum 1000", 400
    schema_dict = {
        "departments": FileSchemas().dept_schema(),
        "jobs": FileSchemas().job_schema(),
        "employees": FileSchemas().hired_schema(),
        }

    schema = schema_dict.get(schema_type, None)
    if schema is None:
        return "invalid schema", 400

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
    return 'INSERTED BATCHS', 200

if __name__ == '__main__':
    app.run(port=8000)