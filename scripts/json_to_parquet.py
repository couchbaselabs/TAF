"""
Convert json files to parquet format.
Path can be a directory containing subdirectories or json files.
Assumption is that the path contains only valid json files.
"""

import os
import re
import shutil
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import *

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("path", help="dir path or json file path, "
                                     "pass multiple paths seperated by comma")

    args = parser.parse_args()
    converter = JSONTOPARQUET()
    paths = args.path.split(",")
    for path in paths:
        converter.convert_json_to_parquet(path)
    print("All files converted to parquet files")

class JSONTOPARQUET():
    """
    This class is used to generate files that are to be uploaded on AWS S3.
    """

    def __init__(self):
        self.file_paths = list()
        self.input_extension = ".json"
        self.output_extension = ".parquet"

    def convert_json_to_parquet(self, path):
        if os.path.isdir(path):
            for subdir, dirs, files in os.walk(path):
                for file in files:
                    self.file_paths.append((subdir, file))
        else:
            self.file_paths.append(("", path))

        spark = SparkSession.builder.master("local[1]").appName("JSON to Parquet").getOrCreate()

        for file_info in self.file_paths:
            file_path = os.path.join(file_info[0], file_info[1])

            if os.stat(file_path).st_size > 0:
                dataframe = spark.read.json(file_path)
            else:
                # In case of empty json files, just create a parquet file
                # with schema. As a schema less parquet file is not a valid
                # parquet file.
                emp_RDD = spark.sparkContext.emptyRDD()
                columns = StructType([
                    StructField('filename', StringType(), True),
                    StructField('folder', StringType(), True),
                    StructField('mutated', IntegerType(), True),
                    StructField('missing_field', IntegerType(), True),
                    StructField('null_key', StringType(), True)
                ])
                dataframe = spark.createDataFrame(data=emp_RDD, schema=columns)
            dataframe.write.parquet(
                file_path[0:len(file_path) - len(self.input_extension)])
            os.remove(file_path)

        # we only need the file ending with .parquet
        regex = re.compile(".*.parquet$")

        for file_info in self.file_paths:

            # When spark writes the output, the file name provided becomes
            # a folder, and in it are the generated parts by spark.
            # The content includes the .parquet, the .parquet.crc and
            # successful or failure and the .crc respective.
            # We only need the .parquet result, we will move it from int-output to output
            # Example
            # data/int-output/real-file-name/part-000.parquet
            # will be moved to:
            # data/output/real-file-name.parquet
            file_path = os.path.join(file_info[0], file_info[1])
            parquet_dir_path = file_path[0:len(file_path) - len(
                self.input_extension)]
            desiredFileName = parquet_dir_path + self.output_extension
            filesInDir = os.listdir(parquet_dir_path)
            generatedFileName = list(filter(regex.match, filesInDir))[0]
            shutil.move(
                os.path.join(parquet_dir_path, generatedFileName),
                desiredFileName)
            shutil.rmtree(parquet_dir_path)

if __name__ == "__main__":
    main()
