---
layout: post
title:  "Curso de Spark e Python: Aula 04 - Operações Básicas"
categories: [ curso_spark ]
author: adriano
permalink: "/spark/aula04_basic_operations.html"
image: "https://penseemti.com.br/wp-content/uploads/2017/02/A-importa%CC%82ncia-do-Banco-de-Dados.jpg"
tag: [featured]
---

# Curso de Spark e Python
## Aula 04 - Operações Bàsicas


```python
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Aula04").getOrCreate()
```

Vamos baixar o dataset desta aula.


```python
!wget 'http://adrianoavelar.com/uploads/datasets/appl_stock.csv' -O stock.csv
```

    --2020-06-26 11:31:52--  http://adrianoavelar.com/uploads/datasets/appl_stock.csv
    Resolving adrianoavelar.com (adrianoavelar.com)... 107.180.27.152
    Connecting to adrianoavelar.com (adrianoavelar.com)|107.180.27.152|:80... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 143130 (140K) [text/csv]
    Saving to: ‘stock.csv’
    
    stock.csv             9%[>                   ]  13,62K  --.-KB/s    eta 4m 42s ^C


### Carregando os dados em `.csv` para `DataFrame Spark`
> Pede para inferir o Schema e informa que o arquivo possui cabeçalho



```python
df = spark.read.csv('stock.csv', inferSchema = True, header = True )
```


```python
df.printSchema()
```

    root
     |-- Date: timestamp (nullable = true)
     |-- Open: double (nullable = true)
     |-- High: double (nullable = true)
     |-- Low: double (nullable = true)
     |-- Close: double (nullable = true)
     |-- Volume: integer (nullable = true)
     |-- Adj Close: double (nullable = true)
    


Vamos dar uma olhada na tabela usando o `pandas`.<br>
Lembre-se que o pandas não é a forma correta de manipular dados distribuidos em Big Data. Mas podemos utilizá-lo para algumas visualizações pequenas.<br>
Recomendo utilizar o `limit` para limitar a tabela antes de usar o `pandas`.<br>


```python
df.limit(10).toPandas()
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
      <th>Date</th>
      <th>Open</th>
      <th>High</th>
      <th>Low</th>
      <th>Close</th>
      <th>Volume</th>
      <th>Adj Close</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2010-01-04</td>
      <td>213.429998</td>
      <td>214.499996</td>
      <td>212.380001</td>
      <td>214.009998</td>
      <td>123432400</td>
      <td>27.727039</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2010-01-05</td>
      <td>214.599998</td>
      <td>215.589994</td>
      <td>213.249994</td>
      <td>214.379993</td>
      <td>150476200</td>
      <td>27.774976</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2010-01-06</td>
      <td>214.379993</td>
      <td>215.230000</td>
      <td>210.750004</td>
      <td>210.969995</td>
      <td>138040000</td>
      <td>27.333178</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2010-01-07</td>
      <td>211.750000</td>
      <td>212.000006</td>
      <td>209.050005</td>
      <td>210.580000</td>
      <td>119282800</td>
      <td>27.282650</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2010-01-08</td>
      <td>210.299994</td>
      <td>212.000006</td>
      <td>209.060005</td>
      <td>211.980005</td>
      <td>111902700</td>
      <td>27.464034</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2010-01-11</td>
      <td>212.799997</td>
      <td>213.000002</td>
      <td>208.450005</td>
      <td>210.110003</td>
      <td>115557400</td>
      <td>27.221758</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2010-01-12</td>
      <td>209.189995</td>
      <td>209.769995</td>
      <td>206.419998</td>
      <td>207.720001</td>
      <td>148614900</td>
      <td>26.912110</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2010-01-13</td>
      <td>207.870005</td>
      <td>210.929995</td>
      <td>204.099998</td>
      <td>210.650002</td>
      <td>151473000</td>
      <td>27.291720</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2010-01-14</td>
      <td>210.110003</td>
      <td>210.459997</td>
      <td>209.020004</td>
      <td>209.430000</td>
      <td>108223500</td>
      <td>27.133657</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2010-01-15</td>
      <td>210.929995</td>
      <td>211.599997</td>
      <td>205.869999</td>
      <td>205.930000</td>
      <td>148516900</td>
      <td>26.680198</td>
    </tr>
  </tbody>
</table>
</div>



## Filtrando os dados

O comando `filter` é usado para filtrar dados. Existem várias formas de filtros. Primeiramente vamos ver o `modo SQL de filtro`.
* Filtrando dados da coluna `Close` menores que `500`


```python
df.filter("Close < 500").limit(100).toPandas()
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
      <th>Date</th>
      <th>Open</th>
      <th>High</th>
      <th>Low</th>
      <th>Close</th>
      <th>Volume</th>
      <th>Adj Close</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2010-01-04</td>
      <td>213.429998</td>
      <td>214.499996</td>
      <td>212.380001</td>
      <td>214.009998</td>
      <td>123432400</td>
      <td>27.727039</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2010-01-05</td>
      <td>214.599998</td>
      <td>215.589994</td>
      <td>213.249994</td>
      <td>214.379993</td>
      <td>150476200</td>
      <td>27.774976</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2010-01-06</td>
      <td>214.379993</td>
      <td>215.230000</td>
      <td>210.750004</td>
      <td>210.969995</td>
      <td>138040000</td>
      <td>27.333178</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2010-01-07</td>
      <td>211.750000</td>
      <td>212.000006</td>
      <td>209.050005</td>
      <td>210.580000</td>
      <td>119282800</td>
      <td>27.282650</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2010-01-08</td>
      <td>210.299994</td>
      <td>212.000006</td>
      <td>209.060005</td>
      <td>211.980005</td>
      <td>111902700</td>
      <td>27.464034</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>95</th>
      <td>2010-05-20</td>
      <td>241.880009</td>
      <td>243.849987</td>
      <td>236.209999</td>
      <td>237.759995</td>
      <td>320728800</td>
      <td>30.804078</td>
    </tr>
    <tr>
      <th>96</th>
      <td>2010-05-21</td>
      <td>232.819988</td>
      <td>244.499989</td>
      <td>231.349995</td>
      <td>242.319992</td>
      <td>305972800</td>
      <td>31.394869</td>
    </tr>
    <tr>
      <th>97</th>
      <td>2010-05-24</td>
      <td>247.279999</td>
      <td>250.900002</td>
      <td>246.260002</td>
      <td>246.759987</td>
      <td>188559700</td>
      <td>31.970113</td>
    </tr>
    <tr>
      <th>98</th>
      <td>2010-05-25</td>
      <td>239.349991</td>
      <td>246.759987</td>
      <td>237.160007</td>
      <td>245.220005</td>
      <td>262001600</td>
      <td>31.770594</td>
    </tr>
    <tr>
      <th>99</th>
      <td>2010-05-26</td>
      <td>250.080009</td>
      <td>252.129990</td>
      <td>243.750011</td>
      <td>244.109993</td>
      <td>212663500</td>
      <td>31.626781</td>
    </tr>
  </tbody>
</table>
<p>100 rows × 7 columns</p>
</div>



* Selecione as colunas `Open` e `Close` apenas quando os dados de `Close` forem maiores que `200`.


```python
df.select(['Open','Close']).filter('Close > 200 ').toPandas()
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
      <th>Open</th>
      <th>Close</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>213.429998</td>
      <td>214.009998</td>
    </tr>
    <tr>
      <th>1</th>
      <td>214.599998</td>
      <td>214.379993</td>
    </tr>
    <tr>
      <th>2</th>
      <td>214.379993</td>
      <td>210.969995</td>
    </tr>
    <tr>
      <th>3</th>
      <td>211.750000</td>
      <td>210.580000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>210.299994</td>
      <td>211.980005</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>144</th>
      <td>252.360008</td>
      <td>253.070011</td>
    </tr>
    <tr>
      <th>145</th>
      <td>252.839993</td>
      <td>249.880005</td>
    </tr>
    <tr>
      <th>146</th>
      <td>249.390007</td>
      <td>249.639999</td>
    </tr>
    <tr>
      <th>147</th>
      <td>251.790009</td>
      <td>245.799992</td>
    </tr>
    <tr>
      <th>148</th>
      <td>242.669987</td>
      <td>239.930000</td>
    </tr>
  </tbody>
</table>
<p>149 rows × 2 columns</p>
</div>



Ao invés de usar a notação SQL. Podemos utilizar o `modo Python de Filtro`<br>
Nesse caso utilizaremos o objeto `pyspark.sql.column.Column`


```python
type(df['Close'])
```




    pyspark.sql.column.Column



O comando de `filtro` é:


```python
df.filter( df['Close'] < 200).toPandas()
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
      <th>Date</th>
      <th>Open</th>
      <th>High</th>
      <th>Low</th>
      <th>Close</th>
      <th>Volume</th>
      <th>Adj Close</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2010-01-22</td>
      <td>206.780006</td>
      <td>207.499996</td>
      <td>197.160000</td>
      <td>197.750000</td>
      <td>220441900</td>
      <td>25.620401</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2010-01-28</td>
      <td>204.930004</td>
      <td>205.500004</td>
      <td>198.699995</td>
      <td>199.289995</td>
      <td>293375600</td>
      <td>25.819922</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2010-01-29</td>
      <td>201.079996</td>
      <td>202.199995</td>
      <td>190.250002</td>
      <td>192.060003</td>
      <td>311488100</td>
      <td>24.883208</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2010-02-01</td>
      <td>192.369997</td>
      <td>196.000000</td>
      <td>191.299999</td>
      <td>194.729998</td>
      <td>187469100</td>
      <td>25.229131</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2010-02-02</td>
      <td>195.909998</td>
      <td>196.319994</td>
      <td>193.379993</td>
      <td>195.859997</td>
      <td>174585600</td>
      <td>25.375533</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2010-02-03</td>
      <td>195.169994</td>
      <td>200.200003</td>
      <td>194.420004</td>
      <td>199.229994</td>
      <td>153832000</td>
      <td>25.812149</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2010-02-04</td>
      <td>196.730003</td>
      <td>198.370001</td>
      <td>191.570005</td>
      <td>192.050003</td>
      <td>189413000</td>
      <td>24.881912</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2010-02-05</td>
      <td>192.630003</td>
      <td>196.000000</td>
      <td>190.850002</td>
      <td>195.460001</td>
      <td>212576700</td>
      <td>25.323710</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2010-02-08</td>
      <td>195.690006</td>
      <td>197.880003</td>
      <td>193.999994</td>
      <td>194.119997</td>
      <td>119567700</td>
      <td>25.150100</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2010-02-09</td>
      <td>196.419996</td>
      <td>197.499994</td>
      <td>194.749998</td>
      <td>196.190004</td>
      <td>158221700</td>
      <td>25.418289</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2010-02-10</td>
      <td>195.889997</td>
      <td>196.600000</td>
      <td>194.260000</td>
      <td>195.120007</td>
      <td>92590400</td>
      <td>25.279660</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2010-02-11</td>
      <td>194.880001</td>
      <td>199.750006</td>
      <td>194.059996</td>
      <td>198.669994</td>
      <td>137586400</td>
      <td>25.739595</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2010-02-23</td>
      <td>199.999998</td>
      <td>201.330002</td>
      <td>195.709993</td>
      <td>197.059998</td>
      <td>143773700</td>
      <td>25.531005</td>
    </tr>
  </tbody>
</table>
</div>



* Seleciona apenas a coluna `Volume` quando a coluna `Low` for menor que 192


```python
df.filter( df['Low'] < 192).select('Volume').toPandas()
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
      <th>Volume</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>311488100</td>
    </tr>
    <tr>
      <th>1</th>
      <td>187469100</td>
    </tr>
    <tr>
      <th>2</th>
      <td>189413000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>212576700</td>
    </tr>
  </tbody>
</table>
</div>



As vezes é necessário filtrar usando múltiplas condições.<br>
Existem duas formas de fazer isso:<br>
1. Criando `filters` em sequência.<br>
    *Nesse caso não temos a ideia do OU condicional*


```python
df.filter( df['Low'] < 192).filter( df['Close'] < 200 ).select( ['Close','Low'] ).toPandas()
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
      <th>Close</th>
      <th>Low</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>192.060003</td>
      <td>190.250002</td>
    </tr>
    <tr>
      <th>1</th>
      <td>194.729998</td>
      <td>191.299999</td>
    </tr>
    <tr>
      <th>2</th>
      <td>192.050003</td>
      <td>191.570005</td>
    </tr>
    <tr>
      <th>3</th>
      <td>195.460001</td>
      <td>190.850002</td>
    </tr>
  </tbody>
</table>
</div>



2. Concatenando multiplas condições<br>
    Podemos utilizar diversos tipos de condicionais. Porém, duas observações são importantes:
    1. Os simbolos de condicionais mudam:<br>
    usar '&' para 'and' <br>
    usar '|' para 'or'<br>
    usar '~' para 'not'<br>
    2. As expressões precisam ser separadas por parênteses.<br>
    Errado: df['Low'] < 192 & df['Close'] < 200<br>
    Certo: (df['Low'] < 192) & (df['Close'] < 200)<br>
    
    

* Selecione as colunas `Close` e `Low` quando **Low** for menor que 192 e **Close** menor que 200


```python
df.filter( (df['Low'] < 192) & (df['Close'] < 200) ).select( ['Close','Low'] ).toPandas()
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
      <th>Close</th>
      <th>Low</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>192.060003</td>
      <td>190.250002</td>
    </tr>
    <tr>
      <th>1</th>
      <td>194.729998</td>
      <td>191.299999</td>
    </tr>
    <tr>
      <th>2</th>
      <td>192.050003</td>
      <td>191.570005</td>
    </tr>
    <tr>
      <th>3</th>
      <td>195.460001</td>
      <td>190.850002</td>
    </tr>
  </tbody>
</table>
</div>



* Selecione os dados quando a coluna `Low` for `197.16`


```python
df.filter(df['Low'] == 197.16).toPandas()
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
      <th>Date</th>
      <th>Open</th>
      <th>High</th>
      <th>Low</th>
      <th>Close</th>
      <th>Volume</th>
      <th>Adj Close</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2010-01-22</td>
      <td>206.780006</td>
      <td>207.499996</td>
      <td>197.16</td>
      <td>197.75</td>
      <td>220441900</td>
      <td>25.620401</td>
    </tr>
  </tbody>
</table>
</div>



Vamos supor que você precise guardar essa informação de Linha em uma variável. Geralmente você deseja trabalhar com o objecto `Row`.<br>
Usa-se a função collect para isso.
> Collect (Ação) - Retorna todos os elementos do conjunto de dados como uma matriz. Isso geralmente é útil após um filtro ou outra operação que retorna um subconjunto suficientemente pequeno dos dados.



```python
row = df.filter(df['Low'] == 197.16).collect()
```


```python
row
```




    [Row(Date=datetime.datetime(2010, 1, 22, 0, 0), Open=206.78000600000001, High=207.499996, Low=197.16, Close=197.75, Volume=220441900, Adj Close=25.620401)]



Pode-se pegar os elementos diretamente com indices de list


```python
row[0][1]
```




    206.78000600000001



Pode-se pegas os resultados como um dicionário


```python
row[0].asDict()
```




    {'Date': datetime.datetime(2010, 1, 22, 0, 0),
     'Open': 206.78000600000001,
     'High': 207.499996,
     'Low': 197.16,
     'Close': 197.75,
     'Volume': 220441900,
     'Adj Close': 25.620401}




```python
row[0].asDict()['High']
```




    207.499996


