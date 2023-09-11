from pyspark.sql import SparkSession
from pySparkMethod import get_product_category_pairs

# Создаем SparkSession
spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()

# Пример данных для продуктов
products_data = [
    (1, "Product G"),
    (2, "Product O"),
    (3, "Product O"),
    (4, "Product D"),
]

# Пример данных для категорий
categories_data = [
    (1, "Category J"),
    (1, "Category O"),
    (2, "Category B"),
    (3, "Category !"),
]

# Создаем датафреймы с использованием SparkSession
products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["product_id", "category_name"])

# Теперь можно использовать функцию get_product_category_pairs
result_df = get_product_category_pairs(products_df, categories_df)
result_df.show()

# Закрываем SparkSession
spark.stop()
