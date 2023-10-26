from pyspark.sql import SparkSession
from pyspark.ml.feature import CountVectorizer, Tokenizer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier, NaiveBayes, LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import nltk

# Download NLTK resources
nltk.download("stopwords", quiet=True)
nltk.download("punkt", quiet=True)
nltk.download("wordnet", quiet=True)

# Initialize SparkSession
spark = SparkSession.builder.appName("EarthquakeDetection").getOrCreate()

# Create UDFs for text preprocessing
def preprocess_tweet(text):
    lemmatizer = nltk.stem.WordNetLemmatizer()
    stemmer = nltk.stem.PorterStemmer()
    words = nltk.word_tokenize(text)
    words = [word.lower() for word in words if word.isalpha()]
    words = [lemmatizer.lemmatize(word) for word in words]
    words = [stemmer.stem(word) for word in words]
    return " ".join(words)

preprocess_tweet_udf = udf(preprocess_tweet)

# Load the dataset as a DataFrame
data = spark.read.json("dataset.json")

# Apply text preprocessing
data = data.withColumn("text", preprocess_tweet_udf(data["text"]))

# Split the data into training and testing sets
train_data, test_data = data.randomSplit([0.8, 0.2], seed=123)

# Create feature vectorizers
count_vectorizer = CountVectorizer(inputCol="text", outputCol="features", binary=True, minDF=0.05)

# Train and evaluate models
classifiers = [
    ("NaiveBayes", NaiveBayes(), BinaryClassificationEvaluator(metricName="areaUnderROC")),
    ("DecisionTree", DecisionTreeClassifier(maxDepth=10), BinaryClassificationEvaluator(metricName="areaUnderROC")),
    ("LinearSVC", LinearSVC(), BinaryClassificationEvaluator(metricName="areaUnderROC"))
]

results = []

for classifier_name, classifier, evaluator in classifiers:
    pipeline = Pipeline(stages=[count_vectorizer, classifier])
    model = pipeline.fit(train_data)
    predictions = model.transform(test_data)
    auc = evaluator.evaluate(predictions)
    results.append((classifier_name, auc))

# Display results
for classifier_name, auc in results:
    print(f"{classifier_name}: AUC = {auc}")

# Stop the SparkSession
spark.stop()
