import csv
import datetime
import json
import logging
import os
import re
import shutil
import tempfile
import time
from typing import List, Dict

import boto3
import numpy as np
import pandas as pd
import PIL
import psutil
import pyarrow.parquet as pq
import pyspark
import pytz

from pandas.testing import assert_frame_equal
from PIL import Image, ImageDraw, ImageFont
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    monotonically_increasing_id,
    regexp_replace,
    row_number,
    to_timestamp,
    trim,
    udf,
    upper,
)
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.window import Window
from typing import Union
from pathlib import Path

pd.DataFrame.iteritems = pd.DataFrame.items

# Start Spark session
def build_spark_session(app_name: str):
    """
    Attempts to ceate and return a spark session.

    parameters:
    -----------
    app_name : str
        Sets a name for the application,
        which will be shown in the Spark web UI.
        If no application name is set, a randomly
        generated name will be used.
        See: SparkSession.Builder docs

    """

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # Use all available cores
        .config("spark.driver.memory", "54G")
        .config("spark.executor.memory", "54G")
        .config("spark.driver.maxResultSize", "4G")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.scheduler.listenerbus.eventqueue.capacity", 40000)  # ---
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -Xss32m")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -Xss32m")
        .config("spark.sql.broadcastTimeout", 2000)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir(str(Path.home()/"scratch"/"checkpoints"))
    return spark

# Function to rename columns
def rename_columns(dataframe, to_rename: List[str] = [], renamed: List[str] = [], rename_map:Dict[str,str] = {}):
    """
    Rename multiple columns of a spark dataframe.

    parameters:
    -----------
    dataframe : pyspark.sql.DataFrame
        A spark dataframe.
    to_rename: List[str]
        A list of existing column names.
    renamed: List[str]
        A list of desired column names.
    rename_map: Dict[str,str]
        A map of existing column names to desired column names. An alternative to using the to_rename and renamed parameters

    :returns: Spark dataframe with column renamed.
    """
    if to_rename and renamed:
        rename_map = {k: v for (k, v) in zip(to_rename, renamed)}
    # select columns with their renamed values as aliases
    dataframe_renamed = dataframe
    for col_name in rename_map:
        dataframe_renamed = dataframe_renamed.withColumnRenamed(
            col_name, rename_map[col_name]
        )

    return dataframe_renamed


def read_file(
    spark,
    path: str,
    delim: str = "|",
    sheet_name: str = None,
    trim_spaces: bool = False,
    delete_invalid_chars: bool = False,
    encoding: str = "ISO-8859-1",
    dtype: str = None,
):
    """
    Read file into pyspark

    parameters:
    -----------
    spark: pyspark.sql.session.SparkSession
        A spark session
    path: str
        Path to file, including the filename. Omit the extension and it will be inferred
    delim: str
        The file delimiter, if the file is a csv
    sheet_name: str
        Which sheet to read (by sheet name), if the file is an excel file. Takes the first sheet by default
    trim_spaces: bool
        Whether to trim whitespace around values in string columns
    delete_invalid_chars: bool
        Deletes quote characters in values in string columns, if file is a csv

    :returns: Spark dataframe
    """
    if "." not in path:
        raise ValueError("Please specify the file extension in the path.")

    SUPPORTED_TYPES = ["parquet", "csv", "xlsx"]
    is_s3_path = path.startswith("s3")
    file_type = str.lower(path.split(".")[-1])

    if file_type not in SUPPORTED_TYPES:
        raise ValueError(f"Unsupported file type: {file_type}")

    # Change s3 URI based on whether we are using pandas or spark
    if is_s3_path:
        # Replace :/ with ://
        path = re.sub(":/(?!/)","://",path)
        s3_protocol = path.split("://")[0]

        if s3_protocol not in ["s3", "s3a"]:
            raise ValueError(f"Unsupported s3 protocol: {s3_protocol}")

        if file_type == "xlsx":
            if s3_protocol == "s3a":
                _, file_path = path.split("://")
                path = f"s3://{file_path}"
        elif s3_protocol != "s3a":
            _, file_path = path.split("://")
            path = f"s3://{file_path}"

    # If s3 path, first download to Triage_Output folder
    if is_s3_path:
        s3_path = path.split("://")[1]
        temp_folder = tempfile.TemporaryDirectory()
        temp_path = temp_folder.name
        path = os.path.join(temp_path, os.path.basename(path))
        bucket_name = s3_path.split("/")[0]
        s3_path = s3_path[len(bucket_name) + 1 :]
        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket(bucket_name)
        paths = [obj.key for obj in bucket.objects.filter(Prefix=s3_path) if not obj.key.endswith("/")]
        if not paths:
            pass
        elif len(paths) == 1:
            bucket.download_file(paths[0], path)
        # If file is a folder, download entire folder
        else:
            os.makedirs(path)
            for path2 in paths:
                filename = os.path.basename(path2)
                bucket.download_file(path2, f"{path}/{filename}")

    if file_type == "parquet":
        df = spark.read.load(path)
        colname_to_dtype = {df.dtypes[i][0]:df.dtypes[i][1] for i in range(len(df.columns))}

        if os.path.isdir(path):
            colname_to_type = {}
            partnames = [partname for partname in os.listdir(path) if partname.endswith(".parquet")]
            # Stores schemas of each partition
            for part_name in partnames:
                schema = pq.read_schema(f"{path}/{part_name}")
                for i in schema:
                    dtype = str(i.type)
                    if i.name not in colname_to_type:
                        colname_to_type[i.name] = {dtype:{part_name}}
                    elif i.name in colname_to_type and dtype not in colname_to_type[i.name]:
                        colname_to_type[i.name][dtype] = {part_name}
                    else:
                        colname_to_type[i.name][dtype] |= {part_name}

            # Checks if any two partitions have differently typed datetime columns
            for colname,dtype_to_partnames in colname_to_type.items():
                if "null" in dtype_to_partnames:
                    dtype_to_partnames.pop("null")
                if len(dtype_to_partnames) >= 2 and [dtype for dtype in dtype_to_partnames if "timestamp" in dtype]:
                    raise Exception(f"Field '{colname}' in {path} has partitions with different data types: {dtype_to_partnames}. Cast these partitions to the same type and then read in again")
            
            true_schema = pq.read_schema(f"{path}/{partnames[0]}")
        else:
            true_schema = pq.read_schema(path)
        for i in range(len(df.columns)):
            col_name = df.columns[i]
            dtype = colname_to_dtype[col_name]
            true_dtype = str(true_schema[i].type)
            if dtype == "bigint" and true_dtype == "timestamp[ns]":
                df = df.withColumn(
                    col_name,
                    udf(
                        lambda unixtime: datetime.datetime.fromtimestamp(
                            unixtime / 1000000000
                        )
                        if unixtime is not None
                        else None,
                        TimestampType(),
                    )(col(col_name)),
                )

    elif file_type == "csv":
        # csvs coming from SPSS Modeler are encoded in ISO-8859-1
        df = spark.read.load(
            path,
            format="csv",
            delimiter=delim,
            header=True,
            inferSchema=True,
            multiLine=True,
            nullValue=None,
            escape="\\",
            timestampFormat="yyyy-MM-dd H:mm",
            encoding=encoding,
        )
    elif file_type == "xlsx":
        if sheet_name:
            df = pd.read_excel(
                path, sheet_name, na_values=["$null$"], keep_default_na=False, dtype=str
            )
        else:
            df = pd.read_excel(
                path, na_values=["$null$"], keep_default_na=False, dtype=str
            )
        df = df.where(df.notnull(), None)
        df = spark.createDataFrame(df)
    else:
        raise TypeError(f"Unsupported filetype: {file_type}")

    for col_name in df.columns:
        new_col_name = (
            col_name.replace("~~SPACE~~", " ")
            .replace("~~COMMA~~", ",")
            .replace("~~QUOTE~~", "'")
            .replace("~~PERIOD~~", ".")
        )
        if col_name != new_col_name:
            df = df.withColumnRenamed(col_name, new_col_name)

    if df.columns[-1][-1] == "\r":
        df = df.withColumnRenamed(df.columns[-1], df.columns[-1][:-1])

    def remove_last_char(col_val):
        if col_val == "\r":
            return None
        elif type(col_val) == str and len(col_val) > 0 and col_val[-1] == "\r":
            return col_val[:-1]
        else:
            return col_val

    if df.dtypes[-1][1] == "string":
        df = df.withColumn(df.columns[-1], udf(remove_last_char)(col(df.columns[-1])))

    for col_name, datatype in df.dtypes:
        if datatype == "string":
            if delete_invalid_chars:
                df = df.withColumn(
                    col_name, regexp_replace(col(col_name), r"(\'|\")", "")
                )
            if trim_spaces:
                df = df.withColumn(col_name, trim(col_name))
            df = df.withColumn(
                col_name, regexp_replace(col(col_name), r"~~NEWLINE~~", "\n")
            )
            df = df.withColumn(
                col_name, regexp_replace(col(col_name), r"~~COMMA~~", ",")
            )
            df = df.withColumn(col_name, regexp_replace(col(col_name), r'\\"', '"'))

    if "__index_level_0__" in df.columns:
        df = df.drop("__index_level_0__")

    if dtype:
        for col_name in dtype:
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(dtype[col_name]))

    if "__ROW_NUMBER__" in df.columns:
        df = df.sort("__ROW_NUMBER__").drop("__ROW_NUMBER__")

    if is_s3_path:
        # Reads dataframe into memory so that it can be deleted from the tmp folder
        df = df.checkpoint()
        temp_folder.cleanup()

    return df


def write_file(spark, df, path, sort_alphabetical=False, forceParquetFile=False, add_parquet_row_num=True):
    """
    Export pyspark file

    parameters:
    -----------
    spark: pyspark.sql.session.SparkSession
        A spark session
    df: pyspark.sql.DataFrame
        A spark dataframe
    path: str
        Directory to export to
    sort_alphabetical: bool
        Whether to sort the dataframe before exporting
    forceParquetFile: bool
        Whether to force partitioned parquet folder into one file
    add_parquet_row_num: bool
        Whether to add __ROW_NUMBER__ column to keep track of row order in parquet file
    returns: the same spark dataframe
    """
    path = str(path)
    filename = os.path.basename(path)
    # Assumes file is a parquet file by default
    if "." not in filename:
        filename += ".parquet"
        path += ".parquet"
    extension = filename.split(".")[-1]
    if extension not in ["csv", "parquet", "xlsx"]:
        raise Exception("Backup type not supported")
    is_s3_path = path.startswith("s3")
    # If s3 path, First download to Triage_Output folder
    if is_s3_path:
        s3_path = path
        temp_folder = tempfile.TemporaryDirectory()
        temp_path = temp_folder.name
        path = os.path.join(temp_path, filename)
    #         path = f"Triage_Output/{filename}"
    if sort_alphabetical:
        df = df.sort(
            [
                upper(col(f"`{col_name2}`"))
                for col_name2 in sorted(df.columns, key=str.casefold)
            ]
        )

    if extension == "parquet":
        # Remove file if it exists
        if os.path.isdir(path):
            for subfilename in os.listdir(path):
                os.remove(Path(path) / subfilename)
            os.rmdir(path)
        elif os.path.isfile(path):
            os.remove(path)
        # Necessary to retain row order, as parquets do not naturally retain row order
        if add_parquet_row_num:
            df = df.withColumn(
                "__ROW_NUMBER__",
                row_number().over(Window.orderBy(monotonically_increasing_id())),
            )
        for col_name in df.columns:
            # Replace invalid characters with a token that will be replaced upon reading back in, so that these characters will not be lost
            new_col_name = (
                col_name.replace(" ", "~~SPACE~~")
                .replace(",", "~~COMMA~~")
                .replace("'", "~~QUOTE~~")
                .replace(".", "~~PERIOD~~")
            )
            if col_name != new_col_name:
                df = df.withColumnRenamed(col_name, new_col_name)
        # Coalesce to 1 partition to export to a parquet file
        if forceParquetFile:
            df = df.coalesce(1)
        
        # Write folder to temp directory
        temp_folder2 = tempfile.TemporaryDirectory()
        temp_path = Path(temp_folder2.name) / filename
        df.write.parquet(str(temp_path))
        # Make parent folder if doesn't exist
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        # Get name of single partition in parquet folder
        parquet_files = [subfilename for subfilename in os.listdir(temp_path) if subfilename.endswith(".parquet")]
        # Move from temp directory to given location
        if len(parquet_files) == 1:
            shutil.move(str(temp_path / parquet_files[0]), path)
        else:
            shutil.move(str(temp_path), path)
        # Delete temp directory
        temp_folder2.cleanup()
        
    elif extension == "csv":
        # Cast all columns to string type, to avoid unintended errors
        for col_name in df.columns:
            df = df.withColumn(col_name, col(f"`{col_name}`").cast(StringType()))
        df.toPandas().to_csv(path, sep="|", index=False, encoding="ISO-8859-1")
    elif extension == "xlsx":
        # Cast all columns to string type, to avoid unintended errors
        for col_name in df.columns:
            df = df.withColumn(col_name, col(f"`{col_name}`").cast(StringType()))
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            df.toPandas().to_excel(writer, index=False, header=True)
            # This allows large files to be saved to excel
            writer.book.use_zip64()
    # Upload file/folder to s3
    if is_s3_path:
        move_to_s3(path, s3_path, delete_dest=True)
        temp_folder.cleanup()

    return df


def move_to_s3(source_path: str, dest_path: str, delete_dest: bool = False) -> None:
    """
    Moves file/folder to s3 bucket

    Args:
        source_path (str): Local path of file/folder to be moved
        dest_path (str): s3 destination path, of form s3a://<bucket-name>/<bucket-path>
        delete_dest (bool): Whether to delete file on s3 if it already exists. Useful for pyspark parquet files
    """
    client = boto3.client("s3")
    # Remove s3://
    dest_path = re.findall("://?(.*)",dest_path)[0]
    # Get bucket name
    bucket_name = dest_path.split("/")[0]
    # Rest of path excluding bucket name
    dest_path = dest_path[len(bucket_name) + 1 :]

    # Delete old files in s3
    if delete_dest:
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=dest_path)
        if "Contents" in response:
            for file in response["Contents"]:
                client.delete_object(Bucket=bucket_name, Key=file["Key"])

    # If source path is file, upload to s3
    if os.path.isfile(source_path):
        client.upload_file(source_path, bucket_name, dest_path)
    # If source path is folder, iterate through each file in folder and upload to s3
    else:
        for root, dirs, files in os.walk(source_path):
            for file in files:
                client.upload_file(
                    os.path.join(root, file), bucket_name, os.path.join(dest_path, file)
                )


def write_read_file(spark, df, path):
    write_file(spark, df, path)
    return read_file(spark, path)


def rename_and_filter_postmerge(
    dataframe,
    keys,
    dataframes,
    dataframe_aliases,
    to_select=None,
    to_drop=None,
    to_rename=[],
    renamed=[],
):
    rename_map = {k: v for (k, v) in zip(to_rename, renamed)}
    new_cols = []
    for key in keys:
        if (
            (to_select and key in to_select)
            or (to_drop and key not in to_drop)
            or (not to_select and not to_drop)
        ):
            new_cols.append(col(key).alias(rename_map.get(key, key)))
    for df, dfname in zip(dataframes, dataframe_aliases):
        for col_name in df.columns:
            if "." in col_name:
                quoted_col_name = "`" + col_name + "`"
            else:
                quoted_col_name = col_name
            alias_col_name = f"{dfname}.{quoted_col_name}"
            if quoted_col_name not in keys and (
                (to_select and alias_col_name in to_select)
                or (to_drop and alias_col_name not in to_drop)
                or (not to_select and not to_drop)
            ):
                new_cols.append(
                    col(alias_col_name).alias(
                        rename_map.get(
                            alias_col_name, rename_map.get(col_name, col_name)
                        )
                    )
                )
    return dataframe.select(new_cols)


def try_persist(
    spark,
    df,
    parent_config_file,
    label,
    persist_counter,
    section=None,
    clear_caches=False,
):
    # Takes persist percent by section (if it exists). Otherwise, takes global persist percent
    if (
        "persist_percent_sections" in parent_config_file
        and section in parent_config_file["persist_percent_sections"]
    ):
        persist_percent = parent_config_file["persist_percent_sections"][section]
    else:
        persist_percent = parent_config_file["persist_percent"]
    # Reset persist counter to zero if its time to persist
    if persist_percent > 0 and persist_counter >= 1 / persist_percent:
        persist_counter = (
            1 + persist_counter - 1 / persist_percent
            if persist_percent > 0
            else persist_counter
        )
        percent_persist = True
    else:
        persist_counter += 1
        percent_persist = False

    # Takes max plan size by section (if it exists). Otherwise, takes global max plan size
    if (
        section
        and "max_plan_size_sections" in parent_config_file
        and section in parent_config_file["max_plan_size_sections"]
    ):
        max_plan_size = parent_config_file["max_plan_size_sections"][section]
    else:
        max_plan_size = parent_config_file["max_plan_size"]

    nodeid = re.findall("(id\w+)\]?$", label)
    if nodeid:
        nodeid = nodeid[0]

    plan_size = len(
        df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")
    )
    ram_str = f"{round(psutil.virtual_memory()[3] / 1000000000, 1)}GB"

    # Persist if:
    #     Spark plan size gets too big
    #     The node is in the persist list
    #     The persist counter reaches 1/persist_percent
    if (
        max_plan_size != "inf"
        and not (
            "persist_list_exclusions" in parent_config_file
            and nodeid in parent_config_file["persist_list_exclusions"]
        )
        and (
            len(
                df._sc._jvm.PythonSQLUtils.explainString(
                    df._jdf.queryExecution(), "formatted"
                )
            )
            > max_plan_size
            or nodeid in parent_config_file["persist_list"]
            or percent_persist
        )
    ):
        start_time = time.perf_counter()
        df = df.checkpoint()
        if parent_config_file["logging_level"] >= 1:
            now_time_str = datetime.datetime.now(
                pytz.timezone("Canada/Eastern")
            ).strftime("%Y-%m-%d %H:%M:%S")
            logging.info(
                f"PERSIST: {label}, {now_time_str}, {time.perf_counter() - start_time}, {df.count()}, {len(df.columns)}, {plan_size}, {ram_str}"
            )
    elif parent_config_file["logging_level"] == 2:
        logging.info(f"""{label} {plan_size} {ram_str}""")

    return (df, persist_counter)


def getTodaysDate(config):
    if config["todays_date"] == "today":
        return datetime.datetime.now(pytz.timezone("Canada/Eastern")).date()
    else:
        return datetime.datetime.strptime(config["todays_date"], "%Y-%m-%d").date()
