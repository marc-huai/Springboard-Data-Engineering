import os
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month


#spark = SparkSession.builder.appName('Refactor').getOrCreate()


base_path = r'C:\Users\march\Downloads'
data_path = os.path.join(base_path, 'data.csv')



#AutosalesDF = spark.read.format('csv').option('header', 'true').load(data_path)
#AutosalesDF.show(5)



sc = SparkContext("local", "AutoInc Spark Job")


#1.1 Read the input data CSV file
raw_rdd = sc.textFile(data_path)  # Update with your HDFS path
raw_rdd.take(5)


'''
Step 1. Filter out accident incidents with make and year
Since we only care about accident records, we should filter out records having incident type
other than “I”. However, accident records don't carry make and year fields due to the design to
remove redundancy. So our first step is propagating make and year info from record type I into
all other record types
'''
# Step 1: Define a function to filter records and map them to key-value pairs
def filter_and_map_records(line):
    fields = line.split(",")
    incident_type = fields[1]
    vin_number = fields[2]
    
    if incident_type == "I":
        # For initial sale records, propagate make and year
        make = fields[3]
        year = fields[5]
        return (vin_number, (incident_type, make, year, line))
    elif incident_type == "A":
        # For accident records, make and year are initially None
        return (vin_number, (incident_type, None, None, line))
    else:
        # Ignore records that are not of interest
        return None

# Apply map function and filter out None values to get key-value RDD
vin_kv = raw_rdd.map(filter_and_map_records).filter(lambda x: x is not None)
vin_kv.take(10)



# Step 1.1: Function to extract key-value pairs from each record
def extract_vin_key_value(line):
    fields = line.split(",")
    incident_type = fields[1]
    vin_number = fields[2]
    
    if incident_type == "I":
        # Initial sale records, propagate make and year
        make = fields[3]
        year = fields[5]
        return (vin_number, (incident_type, make, year, line))
    elif incident_type == "A":
        # Accident records, make and year initially None
        return (vin_number, (incident_type, None, None, line))
    else:
        # Ignore other records
        return None
    
# Apply map function and filter out None values to create key-value RDD
vin_kv = raw_rdd.map(extract_vin_key_value).filter(lambda x: x is not None)
vin_kv.collect()



# Step 1.2: Function to propagate make and year information

def populate_make(records):
    make = None
    year = None
    output = []
    
    # Iterate through records for the same vin_number
    for record in records:
        incident_type, r_make, r_year, line = record
        if incident_type == "I":  # Initial sale record
            make = r_make
            year = r_year
        elif incident_type == "A":  # Accident record
            # Propagate make and year to accident records
            new_line = line.split(",")
            new_line[3] = make if make else ""
            new_line[5] = year if year else ""
            output.append(",".join(new_line))
    return output
'''
def populate_make(records):
    make = None
    year = None
    output = []
    
    # First pass: find the initial sale record and capture make and year
    for record in records:
        incident_type, r_make, r_year, line = record
        if incident_type == "I":  # If initial sale record, capture make and year
            make = r_make
            year = r_year
            output.append(line)  # Keep the initial sale record itself in the output

    # Second pass: propagate make and year to accident records
    for record in records:
        incident_type, _, _, line = record
        if incident_type == "A":  # If accident record
            new_line = line.split(",")
            new_line[3] = make if make else ""  # Propagate make
            new_line[5] = year if year else ""  # Propagate year
            output.append(",".join(new_line))  # Add the modified accident record to the output

    return output
'''


# Group by vin_number and propagate make and year to accident records
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
enhance_make.collect()




# Step 2.1: Function to extract make-year key for counting
def extract_make_key_value(line):
    fields = line.split(",")
    make = fields[3]
    year = fields[5]
    return (f"{make}-{year}", 1)

# Create key-value pairs for counting
make_kv = enhance_make.map(extract_make_key_value)
make_kv.collect()


# Step 2.2: Perform the reduceByKey operation to count accidents by make and year
result = make_kv.reduceByKey(lambda x, y: x + y)
result.collect()




# Step 3: Save the result to HDFS
output_path = os.path.join(base_path, 'data_output.csv')
result.saveAsTextFile(output_path)  # Update with your HDFS output path


#Step 4. Shell script to run the Spark jobs
#spark-submit autoinc_spark.py


