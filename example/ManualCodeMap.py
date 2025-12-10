manual_code_map = {
    "id6S3UFIT3UUB":'''
<df> = <df>.withColumn(
    "DeletePrecedingProvince", regexp_replace(col("processedCity"),"[\w]*(?=\s{1,}province)","")
)
''',
    "id3RYU9G3WVNG":'''
<df> = <df>.withColumn(
    "processedProvince", regexp_replace(col("DeletePrecedingProvince"), "province", "")
)
'''
}