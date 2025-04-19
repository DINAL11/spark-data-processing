from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("ML Pipeline").getOrCreate()

# Load data
df = spark.read.parquet("cleaned_data")

# Create binary label (if purchases > 0 → likely buyer)
df = df.withColumn("label", (df.purchases > 0).cast("int"))

# Feature selection
features = ["age", "income", "clicks", "CTR"]
assembler = VectorAssembler(inputCols=features, outputCol="features")

# ML Model
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=10)

pipeline = Pipeline(stages=[assembler, rf])
model = pipeline.fit(df)

# Print feature importances
rf_model = model.stages[-1]
print("Feature Importances:", rf_model.featureImportances)

spark.stop()
