# Fixtures

Configs that are read by tests on both sides of the project, so neither side can
drift from the other.

## `formats/`

The six native formats — `parquet`, `orc`, `json`, `csv`, `txt`, `view` — as a
pair of pipeline JSONs each: `write.json` puts the seed into the format, and
`read.json` reads it back out. They are the only configs here pinned twice:

- `src/lib/compiler/compiler.test.ts` says the **Studio** can open each one on a
  canvas and compile it back unchanged.
- `server/test_formats_studio_spark.py` says the **runner** really runs them —
  real files written, real rows read back — through the same code `/run` and
  `/run/flow/stream` call. It needs pyspark and a JVM, and skips without them.

A JSON that round-trips perfectly and does not run is still broken, which is why
one half is not enough.

Paths are written as `{dir}/...`, the framework's parameter syntax; the Spark
test passes a temporary directory. `view/` is the exception: it registers a temp
view instead of touching disk, which is how one Pipeline stage hands data to the
next inside one SparkSession.
