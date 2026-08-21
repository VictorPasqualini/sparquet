import sys; sys.path.insert(0,'.')
from sparquet import Sparquet
fw=Sparquet(spark={"app_name":"reportbug","master":"local[*]",
  "configs":{"spark.sql.execution.pyspark.udf.faulthandler.enabled":"true",
             "spark.python.worker.faulthandler.enabled":"true"}})
r=fw.run_from_dict({
 "name":"report_csv",
 "input":{"format":"csv","path":"/data/landing/customers","options":{"header":"true","inferSchema":"true"}},
 "validations":{"on_failure":"warn",
   "rules":[{"type":"not_null","columns":["email"]},{"type":"row_count","min":1}],
   "report":{"format":"csv","path":"/data/dq/report","mode":"overwrite"}},
 "output":{"format":"parquet","path":"/data/curated/rep","mode":"overwrite"}})
print("RESULTADO:", r.summary())
fw.stop()
