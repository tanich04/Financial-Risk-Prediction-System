
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Assemble features
assembler = VectorAssembler(inputCols=['loan_amnt', 'annual_inc', 'dti', 'loan_income_ratio'], outputCol='features')
train_data = assembler.transform(train_data)
test_data = assembler.transform(test_data)

# Train Random Forest model
rf = RandomForestClassifier(featuresCol='features', labelCol='label')
model = rf.fit(train_data)

# Make predictions
predictions = model.transform(test_data)

# Evaluate model
evaluator = BinaryClassificationEvaluator(labelCol='label', rawPredictionCol='prediction', metricName='areaUnderROC')
roc_auc = evaluator.evaluate(predictions)
print(f'ROC AUC: {roc_auc}')
