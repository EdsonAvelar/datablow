---
layout: post
title:  "First Notebook"
categories: [ Analise ]
image: assets/images/demo1.jpg
tag: [featured]
---

# Analisando Spark via Notebook

## Author: Adriano Avelar


```python
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
```


```python
df = spark.read.csv("2019_Viagem.utf8.csv",header=True,sep=";")
```


```python
df.printSchema()
```

    root
     |-- Identificador do processo de viagem: string (nullable = true)
     |-- Número da Proposta (PCDP): string (nullable = true)
     |-- Situação: string (nullable = true)
     |-- Viagem Urgente: string (nullable = true)
     |-- Justificativa Urgência Viagem: string (nullable = true)
     |-- Código do órgão superior: string (nullable = true)
     |-- Nome do órgão superior: string (nullable = true)
     |-- Código órgão solicitante: string (nullable = true)
     |-- Nome órgão solicitante: string (nullable = true)
     |-- CPF viajante: string (nullable = true)
     |-- Nome: string (nullable = true)
     |-- Cargo: string (nullable = true)
     |-- Função: string (nullable = true)
     |-- Descrição Função: string (nullable = true)
     |-- Período - Data de início: string (nullable = true)
     |-- Período - Data de fim: string (nullable = true)
     |-- Destinos: string (nullable = true)
     |-- Motivo: string (nullable = true)
     |-- Valor diárias: string (nullable = true)
     |-- Valor passagens: string (nullable = true)
     |-- Valor outros gastos: string (nullable = true)
    



```python
df.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Identificador do processo de viagem</th>
      <th>Número da Proposta (PCDP)</th>
      <th>Situação</th>
      <th>Viagem Urgente</th>
      <th>Justificativa Urgência Viagem</th>
      <th>Código do órgão superior</th>
      <th>Nome do órgão superior</th>
      <th>Código órgão solicitante</th>
      <th>Nome órgão solicitante</th>
      <th>CPF viajante</th>
      <th>...</th>
      <th>Cargo</th>
      <th>Função</th>
      <th>Descrição Função</th>
      <th>Período - Data de início</th>
      <th>Período - Data de fim</th>
      <th>Destinos</th>
      <th>Motivo</th>
      <th>Valor diárias</th>
      <th>Valor passagens</th>
      <th>Valor outros gastos</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0000000000015045825</td>
      <td>Sem informação</td>
      <td>Realizada</td>
      <td>NÃO</td>
      <td>Sem informação</td>
      <td>26000</td>
      <td>Ministério da Educação</td>
      <td>26291</td>
      <td>Fundação Coordenação de Aperfeiçoamento de Pes...</td>
      <td>***.377.624-**</td>
      <td>...</td>
      <td>None</td>
      <td>-1</td>
      <td>Não Informado</td>
      <td>06/02/2019</td>
      <td>07/02/2019</td>
      <td>Recife/PE</td>
      <td>Regresso de bolsista CAPES do exterior- PE ( P...</td>
      <td>0,00</td>
      <td>3406,33</td>
      <td>0,00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0000000000015100682</td>
      <td>Sem informação</td>
      <td>Realizada</td>
      <td>NÃO</td>
      <td>Sem informação</td>
      <td>26000</td>
      <td>Ministério da Educação</td>
      <td>26291</td>
      <td>Fundação Coordenação de Aperfeiçoamento de Pes...</td>
      <td>***.831.975-**</td>
      <td>...</td>
      <td>None</td>
      <td>-1</td>
      <td>Não Informado</td>
      <td>01/02/2019</td>
      <td>02/02/2019</td>
      <td>Recife/PE</td>
      <td>Capacitação PDSE (Programa de Doutorado Sanduí...</td>
      <td>0,00</td>
      <td>2925,83</td>
      <td>0,00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0000000000015114708</td>
      <td>Sem informação</td>
      <td>Realizada</td>
      <td>NÃO</td>
      <td>Sem informação</td>
      <td>26000</td>
      <td>Ministério da Educação</td>
      <td>26291</td>
      <td>Fundação Coordenação de Aperfeiçoamento de Pes...</td>
      <td>***.325.718-**</td>
      <td>...</td>
      <td>PESQUISADOR EM GEOCIENCIA</td>
      <td>-1</td>
      <td>Não Informado</td>
      <td>01/02/2019</td>
      <td>01/02/2019</td>
      <td>São Paulo/SP</td>
      <td>Capacitação no exterior - PDSE</td>
      <td>0,00</td>
      <td>2760,02</td>
      <td>0,00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0000000000015163874</td>
      <td>Sem informação</td>
      <td>Realizada</td>
      <td>NÃO</td>
      <td>Sem informação</td>
      <td>26000</td>
      <td>Ministério da Educação</td>
      <td>26291</td>
      <td>Fundação Coordenação de Aperfeiçoamento de Pes...</td>
      <td>***.003.005-**</td>
      <td>...</td>
      <td>PROFESSOR DO MAGISTERIO SUPERIOR</td>
      <td>-1</td>
      <td>Não Informado</td>
      <td>17/02/2019</td>
      <td>18/02/2019</td>
      <td>Salvador/BA</td>
      <td>Programa de Professor Visitante no Exterior - ...</td>
      <td>0,00</td>
      <td>2875,92</td>
      <td>0,00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0000000000015166192</td>
      <td>Sem informação</td>
      <td>Realizada</td>
      <td>NÃO</td>
      <td>Sem informação</td>
      <td>26000</td>
      <td>Ministério da Educação</td>
      <td>26291</td>
      <td>Fundação Coordenação de Aperfeiçoamento de Pes...</td>
      <td>***.660.311-**</td>
      <td>...</td>
      <td>TECNICO I</td>
      <td>-1</td>
      <td>Não Informado</td>
      <td>20/02/2019</td>
      <td>21/02/2019</td>
      <td>Rio de Janeiro/RJ</td>
      <td>Capacitação no exterior - PDSE.</td>
      <td>0,00</td>
      <td>2420,48</td>
      <td>0,00</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 21 columns</p>
</div>




```python
## Identificando o orgão que mais gasta dinheiro
```


```python
df.select("Nome do órgão superior", "Valor diárias").show()
```

    +----------------------+-------------+
    |Nome do órgão superior|Valor diárias|
    +----------------------+-------------+
    |  Ministério da Edu...|         0,00|
    |  Ministério da Edu...|         0,00|
    |  Ministério da Edu...|         0,00|
    |  Ministério da Edu...|         0,00|
    |  Ministério da Edu...|         0,00|
    |  Ministério da Edu...|         0,00|
    |  Ministério da Edu...|         0,00|
    |  Ministério da Defesa|         0,00|
    |  Ministério da Defesa|       481,65|
    |  Ministério da Defesa|       456,30|
    |  Ministério da Edu...|      1371,50|
    |  Ministério da Jus...|         0,00|
    |  Ministério da Jus...|         0,00|
    |  Ministério da Edu...|         0,00|
    |  Ministério da Edu...|         0,00|
    |  Ministério da Edu...|         0,00|
    |   Ministério da Saúde|         0,00|
    |  Ministério da Edu...|         0,00|
    |  Ministério da Edu...|         0,00|
    |  Ministério das Re...|         0,00|
    +----------------------+-------------+
    only showing top 20 rows
    



```python
def to_value(v):
    try:
        if (v):
            return float(v.replace(",","."))
        else: 
            return 0.0
    except: 
        return 0.0

udf_to_value = F.udf(to_value, FloatType() )
```


```python
df.withColumn( "ValorDiarias", udf_to_value(df['Valor diárias'] ) ).show(2)
```

    +-----------------------------------+-------------------------+---------+--------------+-----------------------------+------------------------+----------------------+------------------------+----------------------+--------------+--------------------+-----+------+----------------+------------------------+---------------------+---------+--------------------+-------------+---------------+-------------------+------------+
    |Identificador do processo de viagem|Número da Proposta (PCDP)| Situação|Viagem Urgente|Justificativa Urgência Viagem|Código do órgão superior|Nome do órgão superior|Código órgão solicitante|Nome órgão solicitante|  CPF viajante|                Nome|Cargo|Função|Descrição Função|Período - Data de início|Período - Data de fim| Destinos|              Motivo|Valor diárias|Valor passagens|Valor outros gastos|ValorDiarias|
    +-----------------------------------+-------------------------+---------+--------------+-----------------------------+------------------------+----------------------+------------------------+----------------------+--------------+--------------------+-----+------+----------------+------------------------+---------------------+---------+--------------------+-------------+---------------+-------------------+------------+
    |                0000000000015045825|           Sem informação|Realizada|           NÃO|               Sem informação|                   26000|  Ministério da Edu...|                   26291|  Fundação Coordena...|***.377.624-**|MARINA FERREIRA K...| null|    -1|   Não Informado|              06/02/2019|           07/02/2019|Recife/PE|Regresso de bolsi...|         0,00|        3406,33|               0,00|         0.0|
    |                0000000000015100682|           Sem informação|Realizada|           NÃO|               Sem informação|                   26000|  Ministério da Edu...|                   26291|  Fundação Coordena...|***.831.975-**|JORGE ANDRE DE CA...| null|    -1|   Não Informado|              01/02/2019|           02/02/2019|Recife/PE|Capacitação PDSE ...|         0,00|        2925,83|               0,00|         0.0|
    +-----------------------------------+-------------------------+---------+--------------+-----------------------------+------------------------+----------------------+------------------------+----------------------+--------------+--------------------+-----+------+----------------+------------------------+---------------------+---------+--------------------+-------------+---------------+-------------------+------------+
    only showing top 2 rows
    



```python
df = df.withColumn( "ValorDiarias", udf_to_value(df['Valor diárias'] ) ) \
    .withColumn( "ValorPassagens", udf_to_value(df['Valor passagens'] ) ) \
    .withColumn( "ValorOutros", udf_to_value(df['Valor outros gastos'] ) ) 
```


```python
df = df.withColumn("ValorTotal", df['ValorDiarias'] + df['ValorPassagens'] + df['ValorOutros'] )
```


```python
df.printSchema()
```

    root
     |-- Identificador do processo de viagem: string (nullable = true)
     |-- Número da Proposta (PCDP): string (nullable = true)
     |-- Situação: string (nullable = true)
     |-- Viagem Urgente: string (nullable = true)
     |-- Justificativa Urgência Viagem: string (nullable = true)
     |-- Código do órgão superior: string (nullable = true)
     |-- Nome do órgão superior: string (nullable = true)
     |-- Código órgão solicitante: string (nullable = true)
     |-- Nome órgão solicitante: string (nullable = true)
     |-- CPF viajante: string (nullable = true)
     |-- Nome: string (nullable = true)
     |-- Cargo: string (nullable = true)
     |-- Função: string (nullable = true)
     |-- Descrição Função: string (nullable = true)
     |-- Período - Data de início: string (nullable = true)
     |-- Período - Data de fim: string (nullable = true)
     |-- Destinos: string (nullable = true)
     |-- Motivo: string (nullable = true)
     |-- Valor diárias: string (nullable = true)
     |-- Valor passagens: string (nullable = true)
     |-- Valor outros gastos: string (nullable = true)
     |-- ValorDiarias: float (nullable = true)
     |-- ValorPassagens: float (nullable = true)
     |-- ValorOutros: float (nullable = true)
     |-- ValorTotal: float (nullable = true)
    



```python
df_pandas = df.groupBy("Nome do órgão superior").agg(F.sum("ValorPassagens").alias("Total")).orderBy("Total", ascending=False).toPandas()
```


```python
def format(x):
    if(x < 999999):
        return "R$ {:.1f} K".format(x/1000)
    else:
        return "R$ {:.1f} M".format(x/1000000)
    
    
df_pandas['TotalF'] = df_pandas['Total'].apply(format)
```


```python
df_pandas[ ['Nome do órgão superior','TotalF'] ]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Nome do órgão superior</th>
      <th>TotalF</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Ministério da Educação</td>
      <td>R$ 110.6 M</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Ministério da Defesa</td>
      <td>R$ 81.6 M</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Sem informação</td>
      <td>R$ 54.6 M</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Ministério da Justiça e Segurança Pública</td>
      <td>R$ 47.4 M</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Ministério da Saúde</td>
      <td>R$ 33.6 M</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Ministério da Economia</td>
      <td>R$ 28.1 M</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Ministério das Relações Exteriores</td>
      <td>R$ 20.0 M</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Ministério da Infraestrutura</td>
      <td>R$ 18.9 M</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Ministério da Ciência, Tecnologia, Inovações e...</td>
      <td>R$ 16.4 M</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Ministério do Meio Ambiente</td>
      <td>R$ 16.3 M</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Ministério da Agricultura, Pecuária e Abasteci...</td>
      <td>R$ 16.2 M</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Presidência da República</td>
      <td>R$ 15.1 M</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Ministério de Minas e Energia</td>
      <td>R$ 11.1 M</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Ministério da Cidadania</td>
      <td>R$ 5.3 M</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Ministério do Desenvolvimento Regional</td>
      <td>R$ 4.4 M</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Controladoria-Geral da União</td>
      <td>R$ 3.7 M</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Ministério do Turismo</td>
      <td>R$ 2.6 M</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Advocacia-Geral da União</td>
      <td>R$ 2.3 M</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Ministério do Planejamento, Desenvolvimento e ...</td>
      <td>R$ 1.0 M</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Ministério da Indústria, Comércio Exterior e S...</td>
      <td>R$ 928.1 K</td>
    </tr>
    <tr>
      <th>20</th>
      <td>Ministério do Trabalho e Emprego</td>
      <td>R$ 651.7 K</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Ministério do Esporte</td>
      <td>R$ 29.0 K</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Ministério da Cultura</td>
      <td>R$ 5.3 K</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Ministério das Cidades</td>
      <td>R$ 0.0 K</td>
    </tr>
  </tbody>
</table>
</div>




```python
import matplotlib
import seaborn as sns
```


```python
plt.figure(figsize=(15,5))
plt.xticks(rotation=45, horizontalalignment='right')
plt.title("Gastos com Viagens em 2019")
ax = sns.barplot(x=df_pandas['Nome do órgão superior'], y=df_pandas['Total'] )
```


![png](PySparkNotebook_16_0.png)



```python
df.select( df['Nome do órgão superior'].alias("Ministerio"), df['ValorTotal']  ).show()
```

    +--------------------+----------+
    |          Ministerio|ValorTotal|
    +--------------------+----------+
    |Ministério da Edu...|   3406.33|
    |Ministério da Edu...|   2925.83|
    |Ministério da Edu...|   2760.02|
    |Ministério da Edu...|   2875.92|
    |Ministério da Edu...|   2420.48|
    |Ministério da Edu...|    1262.5|
    |Ministério da Edu...|   2694.58|
    |Ministério da Defesa|   1236.38|
    |Ministério da Defesa|    1228.0|
    |Ministério da Defesa|   1749.71|
    |Ministério da Edu...|   2357.65|
    |Ministério da Jus...|   1492.03|
    |Ministério da Jus...|   1492.03|
    |Ministério da Edu...|   4287.77|
    |Ministério da Edu...|       0.0|
    |Ministério da Edu...|     672.6|
    | Ministério da Saúde|       0.0|
    |Ministério da Edu...|   2671.65|
    |Ministério da Edu...|       0.0|
    |Ministério das Re...|   3463.12|
    +--------------------+----------+
    only showing top 20 rows
    


# CPF Mais Gastador


```python
df.groupBy("Nome","CPF viajante") \
    .agg( ( F.sum('ValorPassagens') / F.lit(1_000)) \
    .alias("ValorTotal")) \
    .filter("ValorTotal < 10000") \
    .orderBy("ValorTotal",ascending=False) \
    .limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Nome</th>
      <th>CPF viajante</th>
      <th>ValorTotal</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>BENTO COSTA LIMA LEITE DE ALBUQUERQUE JUNIOR</td>
      <td>***.593.277-**</td>
      <td>331.677539</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ROBERTO DE OLIVEIRA CAMPOS NETO</td>
      <td>***.602.017-**</td>
      <td>215.446949</td>
    </tr>
    <tr>
      <th>2</th>
      <td>DECIO FABRICIO ODDONE DA COSTA</td>
      <td>***.112.110-**</td>
      <td>208.874981</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CARLOS ALEXANDRE JORGE DA COSTA</td>
      <td>***.332.127-**</td>
      <td>190.186081</td>
    </tr>
    <tr>
      <th>4</th>
      <td>LUIZ HENRIQUE MANDETTA</td>
      <td>***.421.431-**</td>
      <td>176.541891</td>
    </tr>
    <tr>
      <th>5</th>
      <td>CARLOS EDUARDO QUINTANILHA VAZ DE OLIVEIRA</td>
      <td>***.493.141-**</td>
      <td>165.803019</td>
    </tr>
    <tr>
      <th>6</th>
      <td>PAULO ROBERTO SOARES PACHECO</td>
      <td>***.137.867-**</td>
      <td>156.802657</td>
    </tr>
    <tr>
      <th>7</th>
      <td>NISIA VERONICA TRINDADE LIMA</td>
      <td>***.005.407-**</td>
      <td>152.658510</td>
    </tr>
    <tr>
      <th>8</th>
      <td>PAULO ROBERTO NUNES GUEDES</td>
      <td>***.305.876-**</td>
      <td>151.715039</td>
    </tr>
    <tr>
      <th>9</th>
      <td>THIAGO COUTO CARNEIRO</td>
      <td>***.639.098-**</td>
      <td>149.506928</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.groupBy("Nome","CPF viajante","Nome do órgão superior") \
    .agg( ( (F.sum('ValorPassagens') + F.sum('ValorDiarias') + F.sum('ValorOutros'))  / F.lit(1_000) )  \
    .alias("ValorTotal"), F.count('ValorDiarias').alias("Quantidade")) \
    .filter("ValorTotal < 10000") \
    .orderBy("ValorTotal",ascending=False) \
    .limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Nome</th>
      <th>CPF viajante</th>
      <th>Nome do órgão superior</th>
      <th>ValorTotal</th>
      <th>Quantidade</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Informações protegidas por sigilo</td>
      <td>None</td>
      <td>Controladoria-Geral da União</td>
      <td>1473.157880</td>
      <td>692</td>
    </tr>
    <tr>
      <th>1</th>
      <td>BENTO COSTA LIMA LEITE DE ALBUQUERQUE JUNIOR</td>
      <td>***.593.277-**</td>
      <td>Ministério de Minas e Energia</td>
      <td>468.256670</td>
      <td>69</td>
    </tr>
    <tr>
      <th>2</th>
      <td>DECIO FABRICIO ODDONE DA COSTA</td>
      <td>***.112.110-**</td>
      <td>Ministério de Minas e Energia</td>
      <td>325.460441</td>
      <td>52</td>
    </tr>
    <tr>
      <th>3</th>
      <td>MARCOS CESAR PONTES</td>
      <td>***.971.638-**</td>
      <td>Ministério da Ciência, Tecnologia, Inovações e...</td>
      <td>292.995513</td>
      <td>53</td>
    </tr>
    <tr>
      <th>4</th>
      <td>PAULO ROBERTO SOARES PACHECO</td>
      <td>***.137.867-**</td>
      <td>Ministério de Minas e Energia</td>
      <td>289.159697</td>
      <td>19</td>
    </tr>
    <tr>
      <th>5</th>
      <td>LUIZ HENRIQUE MANDETTA</td>
      <td>***.421.431-**</td>
      <td>Ministério da Saúde</td>
      <td>280.300470</td>
      <td>69</td>
    </tr>
    <tr>
      <th>6</th>
      <td>ROBERTO DE OLIVEIRA CAMPOS NETO</td>
      <td>***.602.017-**</td>
      <td>Ministério da Economia</td>
      <td>270.549879</td>
      <td>40</td>
    </tr>
    <tr>
      <th>7</th>
      <td>THIAGO COUTO CARNEIRO</td>
      <td>***.639.098-**</td>
      <td>Ministério das Relações Exteriores</td>
      <td>269.038618</td>
      <td>22</td>
    </tr>
    <tr>
      <th>8</th>
      <td>MAX WEBER MARQUES PEREIRA</td>
      <td>***.731.241-**</td>
      <td>Ministério da Saúde</td>
      <td>262.905881</td>
      <td>19</td>
    </tr>
    <tr>
      <th>9</th>
      <td>TEREZA CRISTINA CORREA DA COSTA DIAS</td>
      <td>***.694.306-**</td>
      <td>Ministério da Agricultura, Pecuária e Abasteci...</td>
      <td>259.060558</td>
      <td>51</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_bento = df.filter( df['Nome']== 'BENTO COSTA LIMA LEITE DE ALBUQUERQUE JUNIOR'  )
```


```python
df_bento.printSchema()
```

    root
     |-- Identificador do processo de viagem: string (nullable = true)
     |-- Número da Proposta (PCDP): string (nullable = true)
     |-- Situação: string (nullable = true)
     |-- Viagem Urgente: string (nullable = true)
     |-- Justificativa Urgência Viagem: string (nullable = true)
     |-- Código do órgão superior: string (nullable = true)
     |-- Nome do órgão superior: string (nullable = true)
     |-- Código órgão solicitante: string (nullable = true)
     |-- Nome órgão solicitante: string (nullable = true)
     |-- CPF viajante: string (nullable = true)
     |-- Nome: string (nullable = true)
     |-- Cargo: string (nullable = true)
     |-- Função: string (nullable = true)
     |-- Descrição Função: string (nullable = true)
     |-- Período - Data de início: string (nullable = true)
     |-- Período - Data de fim: string (nullable = true)
     |-- Destinos: string (nullable = true)
     |-- Motivo: string (nullable = true)
     |-- Valor diárias: string (nullable = true)
     |-- Valor passagens: string (nullable = true)
     |-- Valor outros gastos: string (nullable = true)
     |-- ValorDiarias: float (nullable = true)
     |-- ValorPassagens: float (nullable = true)
     |-- ValorOutros: float (nullable = true)
     |-- ValorTotal: float (nullable = true)
    



```python
df_bento Período - Data de fim
```


```python
df1 = df.select( df['Nome do órgão superior'].alias("Ministerio"), df['ValorTotal']  )
```


```python

```


```python
df1.select("Ministerio","ValorTotal").show(20,False)
```

    +-----------------------------------------+----------+
    |Ministerio                               |ValorTotal|
    +-----------------------------------------+----------+
    |Ministério da Educação                   |3406.33   |
    |Ministério da Educação                   |2925.83   |
    |Ministério da Educação                   |2760.02   |
    |Ministério da Educação                   |2875.92   |
    |Ministério da Educação                   |2420.48   |
    |Ministério da Educação                   |1262.5    |
    |Ministério da Educação                   |2694.58   |
    |Ministério da Defesa                     |1236.38   |
    |Ministério da Defesa                     |1228.0    |
    |Ministério da Defesa                     |1749.71   |
    |Ministério da Educação                   |2357.65   |
    |Ministério da Justiça e Segurança Pública|1492.03   |
    |Ministério da Justiça e Segurança Pública|1492.03   |
    |Ministério da Educação                   |4287.77   |
    |Ministério da Educação                   |0.0       |
    |Ministério da Educação                   |672.6     |
    |Ministério da Saúde                      |0.0       |
    |Ministério da Educação                   |2671.65   |
    |Ministério da Educação                   |0.0       |
    |Ministério das Relações Exteriores       |3463.12   |
    +-----------------------------------------+----------+
    only showing top 20 rows
    



```python
df1 = df1.dropna()
```


```python
df1.groupBy("Ministerio").agg(F.sum('ValorTotal')).collect()
```




    [Row(Ministerio='Ministério do Meio Ambiente', sum(ValorTotal)=46705912.21670151),
     Row(Ministerio='Ministério da Agricultura, Pecuária e Abastecimento', sum(ValorTotal)=46740423.9723227),
     Row(Ministerio='Ministério da Infraestrutura', sum(ValorTotal)=34970110.89773178),
     Row(Ministerio='Advocacia-Geral da União', sum(ValorTotal)=4597761.991605759),
     Row(Ministerio='Ministério das Cidades', sum(ValorTotal)=0.0),
     Row(Ministerio='Ministério da Cultura', sum(ValorTotal)=6166.6201171875),
     Row(Ministerio='Ministério da Cidadania', sum(ValorTotal)=8259330.027507305),
     Row(Ministerio='Presidência da República', sum(ValorTotal)=27791225.986211777),
     Row(Ministerio='Ministério da Ciência, Tecnologia, Inovações e Comunicações', sum(ValorTotal)=32006252.885410786),
     Row(Ministerio='Ministério da Defesa', sum(ValorTotal)=191583187.3971342),
     Row(Ministerio='Ministério de Minas e Energia', sum(ValorTotal)=16805548.002960205),
     Row(Ministerio='Ministério da Saúde', sum(ValorTotal)=66416140.027842335),
     Row(Ministerio='Ministério da Justiça e Segurança Pública', sum(ValorTotal)=197650512.31858635),
     Row(Ministerio='Ministério do Esporte', sum(ValorTotal)=33924.460205078125),
     Row(Ministerio='Ministério do Turismo', sum(ValorTotal)=4493266.666252136),
     Row(Ministerio='Ministério da Indústria, Comércio Exterior e Serviços', sum(ValorTotal)=1777851.6522979736),
     Row(Ministerio='Ministério da Economia', sum(ValorTotal)=84871548.44955876),
     Row(Ministerio='Ministério do Desenvolvimento Regional', sum(ValorTotal)=9439679.64227295),
     Row(Ministerio='Ministério das Relações Exteriores', sum(ValorTotal)=33805195.96549225),
     Row(Ministerio='Ministério do Trabalho e Emprego', sum(ValorTotal)=1961430.914653778),
     Row(Ministerio='Sem informação', sum(ValorTotal)=251492135.66048026),
     Row(Ministerio='Controladoria-Geral da União', sum(ValorTotal)=7221319.731559753),
     Row(Ministerio='Ministério da Educação', sum(ValorTotal)=229119022.44529694),
     Row(Ministerio='Ministério do Planejamento, Desenvolvimento e Gestão', sum(ValorTotal)=1507916.614971161)]




```python

```


```python
f
```


    ---------------------------------------------------------------------------

    NameError                                 Traceback (most recent call last)

    <ipython-input-185-a9fcd54b25e7> in <module>
    ----> 1 f
    

    NameError: name 'f' is not defined

