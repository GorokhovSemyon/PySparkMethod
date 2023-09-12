from pyspark.sql import SparkSession

def get_product_category_pairs(products_df, categories_df):
    # Создаем SparkSession
    spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()

    # Регистрируем временные таблицы для продуктов и категорий
    products_df.createOrReplaceTempView("products")
    categories_df.createOrReplaceTempView("categories")

    # Создаем запрос SQL для объединения продуктов и категорий
    query = """
    SELECT p.product_name, c.category_name
    FROM products p
    LEFT JOIN categories c
    ON p.product_id = c.product_id
    """

    # Выполняем запрос и получаем результирующий датафрейм
    result_df = spark.sql(query)

    return result_df
