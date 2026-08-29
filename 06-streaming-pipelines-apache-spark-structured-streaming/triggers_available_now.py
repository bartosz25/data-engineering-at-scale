from pyspark.sql import SparkSession

from config import CHECKPOINTS_DIR, DATA_DIR

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    input_data_stream = (spark.readStream
                         .option('maxFilesPerTrigger', 3)
                         .format('text').load(path=DATA_DIR))

    write_data_stream = (input_data_stream.writeStream
                         .trigger(availableNow=True)
                         .option('checkpointLocation',f'{CHECKPOINTS_DIR}/availablenow-trigger')
                         .format('console'))
    write_data_stream.start().awaitTermination()