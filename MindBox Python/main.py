from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Создаем Spark-сессию
spark = SparkSession.builder.getOrCreate()

# Примерные DataFrame'ы
products = spark.createDataFrame([
    (1, "Молоко"),
    (2, "Хлеб"),
    (3, "Сыр"),
    (4, "Масло")
], ["product_id", "product_name"])

categories = spark.createDataFrame([
    (10, "Молочные продукты"),
    (20, "Выпечка")
], ["category_id", "category_name"])

product_category = spark.createDataFrame([
    (1, 10),
    (2, 20),
    (3, 10)
], ["product_id", "category_id"])

# Джоин таблиц: сначала связываем product_category с categories
prod_cat_joined = product_category.join(
    categories,
    on="category_id",
    how="left"
)

# Теперь связываем с products, чтобы получить все продукты, включая без категории
result = products.join(
    prod_cat_joined,
    on="product_id",
    how="left"
).select(
    col("product_name"),
    col("category_name")
)

result.show()
