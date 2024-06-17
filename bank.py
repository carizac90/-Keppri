from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.functions import round

# Crear una sesión de Spark
spark = SparkSession.builder.appName("BankChurnersStarSchema").getOrCreate()

# Definir el esquema del DataFrame con los tipos de datos correctos
schema = StructType([
    StructField("CLIENTNUM", LongType(), True),
    StructField("Attrition_Flag", StringType(), True),
    StructField("Customer_Age", LongType(), True),
    StructField("Gender", StringType(), True),
    StructField("Dependent_count", LongType(), True),
    StructField("Education_Level", StringType(), True),
    StructField("Marital_Status", StringType(), True),
    StructField("Income_Category", StringType(), True),
    StructField("Card_Category", StringType(), True),
    StructField("Months_on_book", LongType(), True),
    StructField("Total_Relationship_Count", LongType(), True),
    StructField("Months_Inactive_12_mon", LongType(), True),
    StructField("Contacts_Count_12_mon", LongType(), True),
    StructField("Credit_Limit", FloatType(), True),
    StructField("Total_Revolving_Bal", LongType(), True),
    StructField("Avg_Open_To_Buy", FloatType(), True),
    StructField("Total_Amt_Chng_Q4_Q1", FloatType(), True),
    StructField("Total_Trans_Amt", LongType(), True),
    StructField("Total_Trans_Ct", LongType(), True),
    StructField("Total_Ct_Chng_Q4_Q1", FloatType(), True),
    StructField("Avg_Utilization_Ratio", FloatType(), True),
    StructField("Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_1", FloatType(), True),
    StructField("Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_2", FloatType(), True)
])

# Cargar el archivo JSON en un DataFrame de PySpark con el esquema definido
file_path = '/home/carizac/carizac/challenge_Globant/Bank_churn.json'  # Cambia esto a la ruta de tu archivo JSON
df = spark.read.schema(schema).json(file_path)

# Diccionario para renombrar las columnas de manera más concisa y sin camel case
column_rename_mapping = {
    "CLIENTNUM": "client_num",
    "Attrition_Flag": "attrition",
    "Customer_Age": "age",
    "Gender": "gender",
    "Dependent_count": "dependents",
    "Education_Level": "education",
    "Marital_Status": "marital_status",
    "Income_Category": "income_category",
    "Card_Category": "card_category",
    "Months_on_book": "months_book",
    "Total_Relationship_Count": "relationships",
    "Months_Inactive_12_mon": "months_inactive",
    "Contacts_Count_12_mon": "contacts",
    "Credit_Limit": "credit_limit",
    "Total_Revolving_Bal": "revolving_bal",
    "Avg_Open_To_Buy": "open_to_buy",
    "Total_Amt_Chng_Q4_Q1": "amt_chng_q4_q1",
    "Total_Trans_Amt": "trans_amt",
    "Total_Trans_Ct": "trans_ct",
    "Total_Ct_Chng_Q4_Q1": "ct_chng_q4_q1",
    "Avg_Utilization_Ratio": "utilization",
    "Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_1": "nb_classifier_1",
    "Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_2": "nb_classifier_2"
}

# Renombrar las columnas del DataFrame
for old_name, new_name in column_rename_mapping.items():
    df = df.withColumnRenamed(old_name, new_name)

# Formatear columnas `float` a dos decimales
float_columns = ["open_to_buy", "utilization", 
                 "amt_chng_q4_q1", "ct_chng_q4_q1", 
                 "nb_classifier_1", "nb_classifier_2"]

for col_name in float_columns:
    df = df.withColumn(col_name, round(df[col_name], 2))

# Crear la tabla de hechos
fact_table = df.select(
    "client_num", "attrition", "months_book", "relationships", "months_inactive", "contacts",
    "credit_limit", "revolving_bal", "open_to_buy", "amt_chng_q4_q1", "trans_amt", "trans_ct",
    "ct_chng_q4_q1", "utilization", "card_category"
)

# Crear la dimensión cliente
dim_customer = df.select(
    "client_num", "age", "gender", "dependents", "education", "marital_status", "income_category"
)

# Crear la dimensión tiempo (ejemplo simplificado)
dim_time = df.select(
    "months_book", "months_inactive"
).distinct()

# Crear la dimensión producto
dim_product = df.select("card_category").distinct()

# Guardar las tablas en CSV para su uso en Power BI
fact_table.write.csv('fact_table.csv', header=True, mode='overwrite')
dim_customer.write.csv('dim_customer.csv', header=True, mode='overwrite')
dim_time.write.csv('dim_time.csv', header=True, mode='overwrite')
dim_product.write.csv('dim_product.csv', header=True, mode='overwrite')

print("Tablas exportadas a CSV")
