# Week 5 Notes
By Chekwei Chia

## To setup Spark
```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

spark.version
```

## To create temp tables
```python
df_trips_data.registerTempTable('trips_data')
```
After that, we can run SQL on `trips_data` table.

```SQL
spark.sql("""
SELECT
    service_type,
    count(1)
FROM
    trips_data
GROUP BY 
    service_type
""").show()
```

## How Sparks work?
To be updated...