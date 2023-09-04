from sqlite3 import Timestamp
import time
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import *
from croniter import croniter
from datetime import datetime, timedelta
import argparse
from py4j.java_gateway import java_import


# получение аргументов командной строки
def get_args():
    parser = createParser()
    namespace = parser.parse_args()
    arguments = namespace.__dict__
    return arguments


def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--date_from", default=datetime.now())
    parser.add_argument("-e", "--date_to", default=datetime.now() + timedelta(days=7))
    return parser


def get_data(from_table, columns):
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    df = spark.table(from_table).select(columns)
    df.show(5)
    return df


# Периодичность крона
@F.udf
def cron_type(cron):
    if cron.find('*') != -1:
        cron_list = cron.split()
        if (cron_list[1] == '*' or '/' in cron_list[1] or '/' in cron_list[0]) and ('/' not in cron_list[0]):
            periodicity = 'ежечасный'
        elif cron_list[2] != '*':
            periodicity = 'ежедневный'
        elif cron_list[2] == '*' and cron_list[1] != '*' and cron_list[4] == '*' and '/' not in cron_list[1]:
            periodicity = 'ежедневный'
        elif cron_list[4] != '*':
            periodicity = 'еженедельный'
        elif '/' in cron_list[0]:
            periodicity = 'ежеминутный'
        else:
            periodicity = 'неизвестно'
        return periodicity

# Получаем датафрейм dag_id, schedule_interval, periodicity из public_stg.dag
def df_from_publicstgdag():
    # Получаем даги и крон в датафрейм
    columns = ['dag_id', 'is_paused', 'is_active', 'schedule_interval']
    df = get_data('public_stg.dag', columns)
    df = df.filter((df.is_paused == 'false') & (df.is_active == 'true'))
    df = df.select(df.dag_id.alias('dag_id_df'), df.schedule_interval)
    # Добавляем колонку с вычесленной периодичностью
    convertUDF = F.udf(lambda z: cron_type(z), StringType())
    df = df.withColumn('periodicity', cron_type(F.col('schedule_interval')))
    df = df.filter(F.col('periodicity') != 'ежеминутный')
    df = df.distinct()
    df.show(5)
    return df


def get_cron_start_time(date_from, date_to, cron):
    # создаем список запусков для крона в заданном интервале
    d_startlist = []
    base = date_from
    iter = croniter(cron, base)
    next = iter.get_next(datetime)
    return next


# Получаем dag_id, start_date, end_date, execution_date, execution_end_date
def df_from_airflowdatadagrun(date_from, date_to):
    # Получаем даги, фактическое время работы и планируемый старт
    columns = ['dag_id', 'start_date', 'end_date', 'data_interval_end']
    df = get_data('airflow_data.dag_run', columns)
    # Отфильтровываем даги которые попадают в период полученный из командной строки
    df = df.withColumn('start_interval', F.lit(date_from)).withColumn('end_interval', F.lit(date_to))
    df = df.filter((F.unix_timestamp('start_date') > F.unix_timestamp('start_interval')) & \
                   (F.unix_timestamp('end_date') <= F.unix_timestamp('end_interval')))
    df = df.withColumn('execution_end_date',
                       F.to_timestamp(F.unix_timestamp('data_interval_end').cast('long') + \
                                      F.unix_timestamp('end_date').cast('long') - \
                                      F.unix_timestamp('start_date').cast('long')))
    return df


def df_from_airflowdatadependencytrees():
    # Получаем даги, родителей и зависимые 1ого порядка
    columns = ['dest_dag_nm', 'source_dag_nm']
    df = get_data('airflow_data.dependency_trees', columns)
    df1 = df.withColumnRenamed('dest_dag_nm', 'depend_dag_nm').withColumnRenamed('source_dag_nm', 'dest_dag_nm_1')
    df1 = df1.distinct().filter(F.col('dest_dag_nm_1').isNotNull())
    df1 = df1.groupBy('dest_dag_nm_1').agg(F.collect_set('depend_dag_nm').alias('depend_dags_nm'))
    df1 = df1.drop('depend_dag_nm')
    df = df.distinct()
    df = df.groupBy('dest_dag_nm').agg(F.collect_set('source_dag_nm').alias('source_dags_nm'))
    df = df.drop('source_dag_nm')
    # Джойним родителей и зависимые
    df = df.join(df1, (df.dest_dag_nm == df1.dest_dag_nm_1), 'left')
    # Преобразуем в списки родительских и зависимых дагов строки
    df = df.withColumn('source_dags', F.concat_ws(' ', 'source_dags_nm')).withColumn('depend_dags',
                                                                                     F.concat_ws(' ', 'depend_dags_nm'))
    df = df.drop('source_dags_nm', 'depend_dags_nm', 'dest_dag_nm_1')
    df = df.distinct()
    df.show(5)
    return df


# Джойним три датафрейма, разделяем получившийся на два с реальным временем и планируемым и объединяем в один через юнион
def result_frame(df, df1, df2):
    df = df.join(df1, (df.dag_id == df1.dag_id_df), 'inner').join(df2, (df.dag_id == df2.dest_dag_nm), 'inner')
    df1 = df.select(F.col('dag_id'),
                    F.col('data_interval_end').alias('plan_start_date'),
                    'execution_end_date',
                    'schedule_interval',
                    'periodicity',
                    'source_dags',
                    'depend_dags').withColumn('plan', F.lit('plan'))
    df = df.drop('dest_dag_nm',
                 'dag_id_df',
                 'data_interval_end',
                 'execution_end_date',
                 'start_interval',
                 'end_interval').withColumn('plan', F.lit('real'))
    df = df.union(df1)
    df = df.distinct()
    df.show(5)
    return df


def write_to_vertica(df, table_name):
    # Записываем в вертику
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    gw = spark.sparkContext._gateway
    java_import(gw.jvm, "VerticaDialect")
    gw.jvm.org.apache.spark.sql.jdbc.JdbcDialects.registerDialect(gw.jvm.VerticaDialect())
    df.write.format('jdbc') \
        .option("url", "jdbc:vertica://10.220.126.48:5433/") \
        .option("driver", "com.vertica.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("database", "TST") \
        .option("user", "dbadmin") \
        .option("password", "TSTpassword") \
        .mode("append") \
        .save()


def run():
    # Получаем период из командной строки
    dt_fmt = '%Y-%m-%d %H:%M:%S'
    args = get_args()
    date_from = datetime.strptime(args['date_from'] + ' ' + '00:00:00', dt_fmt)
    date_to = datetime.strptime(args['date_to'] + ' ' + '00:00:00', dt_fmt)

    df = df_from_publicstgdag()
    df1 = df_from_airflowdatadagrun(date_from, date_to)
    df2 = df_from_airflowdatadependencytrees()
    df = result_frame(df1, df, df2)
    write_to_vertica(df, "public.dags_schedule_test")


if __name__ == '__main__':
    run()
