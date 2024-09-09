import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, split

def select_name_and_age(df):
    df_filter_columns = df[["Name", "Age"]]
    return df_filter_columns

def filter_age(df):
    df = select_name_and_age(df)
    df_filter_age = df[df["Age"] > 30]
    return df_filter_age

def categorize_age(age):
    if age < 30:
        return "Jovem"
    elif 30 <= age <= 40:
        return "Adulto"
    else:
        return "Senior"

def average_calculator(df):
    average_ages = df.groupby('Occupation')['Age'].mean().reset_index()
    average_ages.rename(columns={'Age': 'average_Age'}, inplace=True)

    df = df.merge(average_ages, on='Occupation')

    df['Age_Diff_From_average'] = df['Age'] - df['average_Age']

    return df

def return_age_category(df):
    df['Age_Category'] = df['Age'].apply(categorize_age)
    return df

def return_broadcast_join_dataframe(df_1, df_2):
    df_broadcast_join = df_1.join(broadcast(df_2), on="Occupation", how="left")
    return df_broadcast_join