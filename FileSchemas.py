from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType

class FileSchemas():
    def hired_schema(self):
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("datetime", StringType(), True),
            StructField("departament_id", StringType(), True),
            StructField("job_id", IntegerType(), True)
            ])

    def job_schema(self): 
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("job", StringType(), True)
        ])
    
    def dept_schema(self):
        return StructType([
                StructField("id", IntegerType(), True),
                StructField("departament", StringType(), True)])
    
    