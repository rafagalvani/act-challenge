import methods as methods
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, split

#Crie um DataFrame a partir do seguinte conjunto de dados:

data = [
    ("Alice", 34, "Data Scientist"),
    ("Bob", 45, "Data Engineer"),
    ("Cathy", 29, "Data Analyst"),
    ("David", 35, "Data Scientist")
]
column_names = ["Name", "Age", "Occupation"]

df = pd.DataFrame(data, columns=column_names)



spark = SparkSession.builder \
    .appName("Act Spark App") \
    .getOrCreate()

df_spark = spark.createDataFrame(data, column_names)

print(methods.select_name_and_age(df))

print(methods.filter_age(df))

print(methods.return_age_category(df))

print(methods.average_calculator(df))

#Quando você particiona um DataFrame, os dados são divididos em partes menores (partições) baseadas em uma ou mais colunas, o que facilita a execução paralela e reduz o tempo de processamento pois leitura ou escrita quando informaada
#a partiçãodos dados, acessa diretamente aquela partição, ignorando o restante. Segue exemplo:

#"F:\Github\act-challenge\output"
output_path = input("Insira o caminho do diretório de saída:")
df_spark.write.partitionBy("Occupation").parquet(output_path)

#Broadcast Join é uma técnica de otimização em PySpark usada para melhorar o desempenho de operações de join entre DataFrames quando um deles é pequeno e o outro grande, reduz a necessidade de movimentar grandes volumes de dados entre nós, 
#especialmente quando o DataFrame pequeno pode ser transmitido rapidamente.

data_occupation = [
    ("Data Scientist", "Senior Data Scientist"),
    ("Data Engineer", "Lead Data Engineer"),
    ("Data Analyst", "Junior Data Analyst")
]
columns_occupation = ["Occupation", "Full_Title"]

df_employees = spark.createDataFrame(data, column_names)
df_occupations = spark.createDataFrame(data_occupation, columns_occupation)

print(methods.return_broadcast_join_dataframe(df_employees, df_occupations))

#####

#F:\Github\act-challenge\input\employees.csv
csv_file_path = input("Insira o caminho do diretório contendo o nome do CSV de origem")

df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df.write.parquet("F:\Github\act-challenge\output\employees.parquet", mode='overwrite')

print(f"Parquet criado com sucesso")

###
#hdfs://namenode:9000/input/employees.csv
hdfs_input_path = input("Insira o caminho do arquivo CSV no HDFS:")

df = spark.read.csv(hdfs_input_path, header=True, inferSchema=True)

df.write.parquet("hdfs://namenode:9000/output/employees.csv", mode='overwrite')

print(f"Arquivo gerado no HDFS com sucesso.")

###

log_file_path = "act-challenge/input/activities.log"

df = spark.read.text(log_file_path)

df_split = df.select(
    split(col("value"), ",").getItem(0).alias("timestamp"),
    split(col("value"), ",").getItem(1).alias("user_id"),
    split(col("value"), ",").getItem(2).alias("action")
)

user_action_counts = df_split.groupBy("user_id").count()

top_10_users = user_action_counts.orderBy(col("count").desc()).limit(10)

output_file_path = "act-challenge/output/activities_top_10.csv"

top_10_users.write.csv(output_file_path, header=True, mode='overwrite')

print(f"Arquivo CSV gerado com sucesso")