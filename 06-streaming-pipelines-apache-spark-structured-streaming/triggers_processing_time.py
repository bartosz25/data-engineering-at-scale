from pyspark.sql import SparkSession, functions

from config import CHECKPOINTS_DIR, DATA_DIR

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    input_data_stream = (spark.readStream
        .option('maxFilesPerTrigger', 2)
        .format('text').load(path=DATA_DIR)
        .withColumn('processing_time', functions.current_timestamp()))

    write_data_stream = (input_data_stream.writeStream
                         .trigger(processingTime='30 seconds')
                         .option('checkpointLocation',f'{CHECKPOINTS_DIR}/processing-time-trigger')
                         .format('console').option('truncate', False))
    write_data_stream.start().awaitTermination()
