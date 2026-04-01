from  pyspark.sql import SparkSession
from  pyspark.sql.functions import col, regexp_replace
spark =SparkSession.builder\
.appName("bolsa Familia")\
.getOrCreate()
caminho_csv = "../dados/pagamentos.csv"

df = spark.read\
.option("header",True)\
.option("InterSchema",True)\
.option("sep",";")\
.option("Encoding", "ISO-8859-1")\
.csv(caminho_csv)

df.show(10)
df.printSchema()



#padronização De Dados

df_tratado = df.withColumnRenamed(\
    "MÊS COMPETÊNCIA","mês_competência"
    
    )
df_tratado.show()

# Criando dicionário
df_tratado = df

colunas_padrao = {
    "MÊS COMPETÊNCIA": "mes_competencia",
    "MÊS REFERÊNCIA": "mes_competencia",
    "UF": "uf",
    "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio",
    "NOME MUNICÍPIO": "nome_municipio",
    "CPF FAVORECIDO": "cpf_favorecido",
    "NIS FAVORECIDO": "nis_favorecido",
    "NOME FAVORECIDO": "nome_favorecido",
    "VALOR PARCELA": "valor_parcela"
}

for antiga, nova in colunas_padrao.items():
    df_tratado = df_tratado.withColumnRenamed(antiga, nova)
df_tratado.show()

#tratamento Automático
import unicodedata, re

def padronizar_nomes(col):
    col = col.lower()
    col = unicodedata.normalize("NFD",col)
    col = col.encore("ascii","ignore").decore("utf-8")
    col = re.sub(r"[^a-z0-9]+","_",col)
    col = col.strip("_")
    return col

df_tratado = df_tratado .toDF(
    *[padronizar_nomes(c) for c in df_tratado.columns]
)
#trata valores nulos de uma coluna de tabela especifica

#df_tratado = df_tratado.dropna(subset=["valor_parcela"])
df_tratado = df_tratado.dropna()

#troca vírgula por ponto

df_tratado = df_tratado.withColumn(
    "valor_parcela",
    regexp_replace( col ("valor_parcela"),",",".")

)
df_tratado = df_tratado.withColumn(
    "valor_parcela",
    col ("valor_parcela").cast("decimal(10,2)")



)
media_geral = df_tratado.agg(avg("valor_parcela")).collect()[0][0]
print(f"Para fins de comparação, a média geral de cada parcela é R$ {media_geral:.2f}")


import matplotlib.pyplot as plt

df_uf = df_tratado.groupBy("uf") \
    .agg(sum("valor_parcela").alias("total_pago")) \
    .orderBy("total_pago", ascending=False) \
    .limit(10) \
    .toPandas()

# Gráfico
plt.figure()
plt.bar(df_uf["uf"], df_uf["total_pago"])
plt.title("Top 10 UFs - Total Pago Bolsa Família")
plt.xlabel("UF")
plt.ylabel("Total Pago")
plt.show()