import os
import sys
import re
import subprocess
from datetime import datetime
from pyspark.shell import spark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import StorageLevel
from pyspark.sql.functions import *

DTM = (datetime.now().strftime("%Y%m%d %H%M%S"))
print("Run Date And Time : " + DTM)
input_file_path = ''

# Creating Spark Session
def spark_builder():
    try:
        spark = SparkSession.builder.master("local[1]").appName("csv_processing").enableHiveSupport().getOrCreate()
        return spark
    except Exception as spark_error:
        print("Unable to create spark session. \n {}".format(spark_error))

#Func for regex
def strstandard(aaa):
    zz = re.sub('[0-9-?,/_()\\[\\]]', '', aaa)
    return zz


def run_cmd(args_list):
    print('Running HDFS Command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    (output, errors) = proc.communicate()
    return (output, errors)


def main():
    # Getting Runtime Inputs
    spark_session = spark_builder()
    spark_context = spark_session.sparkContext
    # sc = spark_context
    print("Spark Session created")

    # input_file_path
    input_file_path = "hdfs://localhost:54310/user/hduser/spark_hack/ins*.csv"


    cmd = ['hadoop', 'fs', '-ls', input_file_path]
    (output, error) = run_cmd(cmd)
    print(output)
    #filelist = [line.rsplit(None,1)[-1] for line in output.split('\n') if len(line.rsplit(None,1))][1:]
    filelist = [line.rsplit(None,1)[-1] for line in output.split('\n') if len(line.rsplit(None,1))!=0]
    print(filelist)
    # print("Value " +str(code))
    # if code:
        # print 'file not exist'
	# sys.exit()

    # sys.exit()
    # Getting Input Files For Processing
    print("Input file paath : " + input_file_path)
    input_file_list = filelist
    no_of_files = len(input_file_list)

    if no_of_files == 0:
        print("Terminating the script due to 0 input file")
        exit()

    new_rdd = []
    for x in range(no_of_files):
        file_to_process = input_file_list[x]
        print("Processing file " + str(x + 1) + " of " + str(no_of_files) + " - " + file_to_process)

        process_file = file_to_process
	print(process_file)
        intake_rdd = spark_context.textFile(process_file)

        # getting count of records in rdd
        intake_count = intake_rdd.count()
        print("No of lines in Intake     : " + str(intake_count))

        # Getting Header
        csv_header = intake_rdd.first()

        # Removing Header And Split lines and data cleaning
        input_data_rdd = intake_rdd.filter(lambda line: line != csv_header).filter(lambda x: len(x) > 0) \
            .map(lambda x: x.strip(",,"), 1).map(
            lambda x: x.replace("01-10-2019", "2019-10-01").replace("02-10-2019", "2019-10-02")). \
            map(lambda y: y.split(",", -1))

        accept_data_rdd = input_data_rdd.filter(lambda line: len(line) == 10)
        reject_data_rdd = input_data_rdd.filter(lambda line: len(line) != 10)
        reject_data_rdd.saveAsTextFile('hdfs://localhost:54310/user/hduser/spark_hack/out/rejected_trans_rdd_' + datetime.now().strftime("%Y%m%d %H%M%S"))

        print("No of Input Transactions  : " + str(input_data_rdd.count()))
        print("No of Accept Transactions : " + str(accept_data_rdd.count()))
        print("No of Reject Transactions : " + str(reject_data_rdd.count()))

	# Creating Schema RDD
        accept_schema_rdd = accept_data_rdd.map(lambda p: Row(IssuerId=p[0], IssuerId2=(p[1]),
                                                              BusinessDate=(p[2]),
                                                              StateCode=p[3], SourceName=p[4], NetworkName=p[5],
                                                              NetworkURL=p[6], custnum=p[7], MarketCoverage=p[8],
                                                              DentalOnlyPlan=p[9]))
        new_rdd.append(accept_schema_rdd)

    # Merging RDD's into Single RDD
    ct = 0
    for x in new_rdd:
        ct += 1
	if len(new_rdd)==1:
           globals()['data_rdd_' + str(ct)] = x
	else:
           globals()['new_rdd_' + str(ct)] = x
        # rdd_ls.append("new_rdd_" + str(ct))
        if ct > 1:
            globals()['data_rdd_' + str(ct)] = globals()['new_rdd_' + str(ct - 1)].union(
                globals()['new_rdd_' + str(ct)])

    data_merge_rdd = globals()['data_rdd_' + str(len(new_rdd))]

    print("Count of merged data RDD = " + str(data_merge_rdd.count()))

    data_merge_rdd.cache()

    # Displaying first few rows
    data_merge_rdd.take(2)

    # Removing Duplicated in the merged RDD and repartitioning
    un_dup_rdd = data_merge_rdd.distinct()
    partition_rdd = un_dup_rdd.repartition(8)

    # Saving the RDD created based on Bussiness Date into HDFS
    rdd_20191001 = partition_rdd.filter(lambda l: l[0] == '2019-10-01')
    rdd_20191002 = partition_rdd.filter(lambda l: l[0] == "2019-10-02")
    rdd_20191001.saveAsTextFile('hdfs://localhost:54310/user/hduser/spark_hack/out/accept_rdd_20191001_' + datetime.now().strftime("%Y%m%d %H%M%S"))
    rdd_20191002.saveAsTextFile('hdfs://localhost:54310/user/hduser/spark_hack/out/accept_rdd_20191002_' + datetime.now().strftime("%Y%m%d %H%M%S"))
    print("Count of records in 2019-10-01 : " + str(rdd_20191001.count()))
    print("Count of records in 2019-10-02 : " + str(rdd_20191002.count()))

    # Converting the partitioned rdd into DF
    insure_date_repart_df = partition_rdd.toDF()
    insure_date_repart_df = insure_date_repart_df.withColumn("BusinessDate", col("BusinessDate").cast(DateType()))
    insure_date_repart_df = insure_date_repart_df.withColumn("IssuerId", col("IssuerId").cast(IntegerType()))
    insure_date_repart_df = insure_date_repart_df.withColumn("IssuerId2", col("IssuerId2").cast(IntegerType()))
    
    # Creating Schema for CSV DF intake
    insurance_schema = StructType([StructField("IssuerId", IntegerType(), True), StructField("IssuerId2", IntegerType(), True), StructField("BusinessDate", DateType(), True), StructField("StateCode", StringType(), True), StructField("SourceName", StringType(), True), StructField("NetworkName", StringType(), True),        StructField("NetworkURL", StringType(), True), StructField("custnum", StringType(), True), StructField("MarketCoverage", StringType(), True), StructField("DentalOnlyPlan", StringType(), True)])
    
    # Loading CSV into DF
    insuranceinfo1 = spark.read.format("csv").option("header",True).schema(insurance_schema).load("hdfs://localhost:54310/user/hduser/spark_hack/insuranceinfo1.csv")
    insuranceinfo2 = spark.read.format("csv").option("header",True).schema(insurance_schema).load("hdfs://localhost:54310/user/hduser/spark_hack/insuranceinfo2.csv")

    # Column Renaming
    insuranceinfo1=insuranceinfo1.withColumnRenamed("StateCode","stcd").withColumnRenamed("SourceName","srcnm")
    insuranceinfo2=insuranceinfo2.withColumnRenamed("StateCode","stcd").withColumnRenamed("SourceName","srcnm")

    insuranceinfo1=insuranceinfo1.withColumn("issueridcomposite", concat("IssuerId","IssuerId2").cast(StringType())).withColumn("sysdt", lit(datetime.now().date()).cast(DateType())).withColumn("systs", lit(datetime.now()).cast(TimestampType())).drop("DentalOnlyPlan")
    insuranceinfo1=insuranceinfo1.na.drop("any")

    insuranceinfo2=insuranceinfo2.withColumn("issueridcomposite", concat("IssuerId","IssuerId2").cast(StringType())).withColumn("sysdt", lit(datetime.now().date()).cast(DateType())).withColumn("systs", lit(datetime.now()).cast(TimestampType())).drop("DentalOnlyPlan")
    insuranceinfo2=insuranceinfo2.na.drop("any")
    

    # UDF Function Creation for cleaning Network Name
    strstanudf = udf(lambda a: strstandard(a))

    insuranceinfo1=insuranceinfo1.withColumn("NetworkName_Cleaned", strstanudf(col("NetworkName")))
    insuranceinfo2=insuranceinfo2.withColumn("NetworkName_Cleaned", strstanudf(col("NetworkName")))
    
    print("Merging Data Frames")
    insuranceinfo_df=insuranceinfo1.union(insuranceinfo2)
    print("Removing Duplicates in DataFrame")
    print("Count Before De-Dup: " + str(insuranceinfo_df.count()))
    insuranceinfo_dedup=insuranceinfo_df.distinct()
    print("Count After De-Dup: " + str(insuranceinfo_dedup.count()))
    insuranceinfo_dedup.createOrReplaceTempView("insureview")

    print("Saving the dataframe as JSON into HDFS")
    insuranceinfo_dedup.write.format("json").mode("overwrite").save("hdfs://localhost:54310/user/hduser/spark_hack/df_out/insure_info_json")
    print("Saving the dataframes as CSV into HDFS")
    insuranceinfo_dedup.write.format("csv").mode("overwrite").option("sep", "~").save("hdfs://localhost:54310/user/hduser/spark_hack/df_out/insure_info_csv")
    print("Saving the dataframes into HIVE")
    insuranceinfo_dedup.write.mode("append").saveAsTable("sample.insuranceinfo_hive")

    
    # RDD for Cust State
    cust_st_intake_rdd = spark_context.textFile("hdfs://localhost:54310/user/hduser/spark_hack/custs_states.csv")
    cust_st_header = cust_st_intake_rdd.first()
    cust_st_input_rdd = cust_st_intake_rdd.filter(lambda line: line != cust_st_header).filter(lambda x: len(x) > 0) \
            .map(lambda x: x.strip(",,"), 1).map(lambda y: y.split(",", -1))

    cust_filter_rdd = cust_st_input_rdd.filter(lambda line: len(line) == 5)
    states_filter_rdd = cust_st_input_rdd.filter(lambda line: len(line) == 2)

    # Step 32
    custstatesdf=spark.read.csv("hdfs://localhost:54310/user/hduser/spark_hack/custs_states.csv")
    custstatesdf.printSchema()
    custfilterdf=custstatesdf.where(col("_c2").isNotNull())
    statesfilterdf=custstatesdf.where(col("_c2").isNull())

    statesfilterdf=statesfilterdf.drop(col("_c4"))
    statesfilterdf=statesfilterdf.drop(col("_c3"))
    statesfilterdf=statesfilterdf.drop(col("_c2"))

    custfilterdf=custfilterdf.withColumnRenamed("_c0","CUST_NO").withColumnRenamed("_c1","FIRST_NAME").withColumnRenamed("_c2","LAST_NAME").withColumnRenamed("_c3","AGE").withColumnRenamed("_c4","PROFESSION")
    statesfilterdf=statesfilterdf.withColumnRenamed("_c0","STATE_CD").withColumnRenamed("_c1","STATE_NAME")

    custfilterdf.show()
    statesfilterdf.show()
    statesfilterdf.createOrReplaceTempView("statesview")
    custfilterdf.createOrReplaceTempView("custview")
    custfilterdf.createOrReplaceTempView("custview")


    spark.udf.register("remspecialcharudf", strstandard)

    final_df=spark.sql("select IssuerId, IssuerId2, BusinessDate, stcd, srcnm, NetworkName, NetworkURL, custnum, MarketCoverage, issueridcomposite, remspecialcharudf(NetworkName) as cleannetworkname, current_date() as curdt, current_timestamp() as curts, year(BusinessDate) as yr, month(BusinessDate) as mth,case when NetworkURL like 'http:%' then 'http' when NetworkURL like 'https%' then 'https' else 'noprotocol' end as Protocal, STATE_NAME, AGE, PROFESSION from insureview INS inner join statesview ST on ST.STATE_CD=INS.stcd inner join custview CU on CU.CUST_NO=INS.custnum")
    final_df.show()
    print("Saving the DataFrame as single Parquet file")
    final_df.coalesce(1).write.format("parquet").mode("overwrite").save("hdfs://localhost:54310/user/hduser/spark_hack/df_out/Final_DF")
    print("End of Program")


if __name__ == "__main__":
    main()
