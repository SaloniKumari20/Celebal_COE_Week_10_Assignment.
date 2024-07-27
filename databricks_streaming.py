from pyspark.sql.functions import expr


connectionString = "Your_Input_Event_Hub_Connection_String"
ehConf = {
    'eventhubs.connectionString': connectionString
}


inputStream = (spark.readStream
               .format("eventhubs")
               .options(**ehConf)
               .load())

inputStream = (inputStream
               .withColumn("value", expr("cast(body as string)"))
               .selectExpr("cast(value as int) as value"))


streamWithRisk = (inputStream
                  .withColumn("Risk", expr("CASE WHEN value > 80 THEN 'High' ELSE 'Low' END")))


outputConnectionString = "Your_Output_Event_Hub_Connection_String"
outputEhConf = {
    'eventhubs.connectionString': outputConnectionString
}

query = (streamWithRisk.writeStream
         .format("eventhubs")
         .options(**outputEhConf)
         .option("checkpointLocation", "/path/to/checkpoint/dir")
         .start())

query.awaitTermination()
