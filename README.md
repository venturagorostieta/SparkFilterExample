# SparkFilterExample
Example using spark 2.3 with Filters, load  log.

Example for runnnig application:

spark-submit \
  --class "com.ventur.filter.SparkFilterExample" \
  --master local[*] \
 /tmp/spark/SparkFilter.jar  /tmp/examples/spark/filter/exampleFilter.log   /tmp/examples/spark/filter/outputWarn   /tmp/examples/spark/filter/outputErr
