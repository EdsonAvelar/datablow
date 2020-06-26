---
layout: post
title:  "Curso de Spark e Python: Aula 02 - DataFrame I"
categories: [ curso_spark ]
author: adriano
permalink: "/spark/aula02_dataframe1.html"
image: "https://files.keepingcurrentmatters.com/wp-content/uploads/2020/05/05155523/20200506-KCM-Shar.jpg"
tag: [featured]
---

# Curso de Spark e Python
## Aula 02 - DataFrame I
<br>
> Para essa aula, você já deverá ter um ambiente `spark` instalado e configurado. <br>

O primeiro passo é iniciar uma sessão Spark. Mas antes, precisamos achar o spark que está instalado na máquina. <br>
A biblioteca `findspark` faz esse trabalho para nós. Caso não tenha ela instalado, você pode digitar no terminal:<br>


`pip install findspark` <br>

ou no próprio jupyter: <br>


```python
!pip install findspark
```

    Requirement already satisfied: findspark in /home/adriano/anaconda3/lib/python3.7/site-packages (1.4.2)



```python
import findspark
findspark.init()
```

Soment após o `findspark.init()` que o pacote pyspark estará disponivel para importação (comando a seguir)


```python
from pyspark.sql import SparkSession
```

Agora, vamos criar uma sessão spark.


```python
#appName: Nome da aplicação
#getOrCreate: Pega uma sessão se ela já existir, caso contrário criar uma nova 

spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

```

Agora vamos importar alguns dados de um arquivo e criar um DataFrame Spark. Você pode utilizar o arquivo que desejar, no formato que desejar.<br>
Vamos começar um simples Arquivo `json`:
```
{"name":"Michael"}
{"name":"Andy", "age":30}
{"name":"Justin", "age":19}
```


```python
df = spark.read.json('uploads/data/people.json')
```


```python
df.show()
```

    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    


Perceba que o Spark automaticamente substitui os valores faltosos como `null`<br>
Para imprimir o Schema do DataFrame utilize a função `printSchema`


```python
df.printSchema()
```

    root
     |-- age: long (nullable = true)
     |-- name: string (nullable = true)
    


O Spark também já inferiu os dados de acordo com o tipo que está sendo armazenado. <br>
Outro comando útil é o `columns`, que não é uma função e sim um parâmetro do `dataframe`


```python
df.columns
```




    ['age', 'name']



Para saber um resumo básico estatistico dos dados, podemos usar a função `describe`


```python
df.describe()
```




    DataFrame[summary: string, age: string, name: string]



Aqui vem um detalhe interessante. O Spark usa o princípio `Lazy Evaluation`. Ou seja, ele não executa as transformações até que uma ação é chamada. <br>
Nesse caso, describe é um transformação, portanto, não é executada até que uma ação seja solicitada. Essa estratégia permite acumular transformações e executar todas de uma única vez, o que traz um grande ganho de desempenho. <br><br>
Para ver o resultado do `describe()` use o comando `show()`. Como show é uma ação, as transformações que vem antes serão executadas.


```python
df.describe().show()
```

    +-------+------------------+-------+
    |summary|               age|   name|
    +-------+------------------+-------+
    |  count|                 2|      3|
    |   mean|              24.5|   null|
    | stddev|7.7781745930520225|   null|
    |    min|                19|   Andy|
    |    max|                30|Michael|
    +-------+------------------+-------+
    


Que tal ver os resultados através do Pandas?


```python
df.describe().toPandas()
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
      <th>summary</th>
      <th>age</th>
      <th>name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>count</td>
      <td>2</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>mean</td>
      <td>24.5</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>stddev</td>
      <td>7.7781745930520225</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>min</td>
      <td>19</td>
      <td>Andy</td>
    </tr>
    <tr>
      <th>4</th>
      <td>max</td>
      <td>30</td>
      <td>Michael</td>
    </tr>
  </tbody>
</table>
</div>



Muito melhor não é? <br><br>

⛔ Mas cuidado. O `pandas` só pode ser utilizado quando os resultados são pequenos. Ele não foi feito para grandes massas de dados. Utilize com muita cautela. <br>

Caso você não consiga rodar o comando acima, é possível que você não tenha o pandas em seu computador. O comando a seguir resolve isso.<br>



```python
!pip install pandas
```

    Requirement already satisfied: pandas in /home/adriano/anaconda3/lib/python3.7/site-packages (1.0.1)
    Requirement already satisfied: pytz>=2017.2 in /home/adriano/anaconda3/lib/python3.7/site-packages (from pandas) (2019.3)
    Requirement already satisfied: numpy>=1.13.3 in /home/adriano/anaconda3/lib/python3.7/site-packages (from pandas) (1.18.1)
    Requirement already satisfied: python-dateutil>=2.6.1 in /home/adriano/anaconda3/lib/python3.7/site-packages (from pandas) (2.8.1)
    Requirement already satisfied: six>=1.5 in /home/adriano/anaconda3/lib/python3.7/site-packages (from python-dateutil>=2.6.1->pandas) (1.14.0)


## Criando o próprio Schema

Muitas vezes você necessita que seu dados estejam de tipos diferentes dos que o Spark inferiu. <br>
Por exemplo, em nosso caso, ele inferiu que a coluna `age` é do tipo **long**, se quisermos mudar para **int**, por exemplo, precisamos criar nosso proprio `Schema`.<br>
Então é isso que vamos fazer.<br>
Primeiramente importamos nossos pacotes necessários.


```python
from pyspark.sql.types import (StructField, StringType,
                              IntegerType, StructType)
```

Depois criamos uma lista com `StructFields` informando 3 parâmetros em cada:
* 1: Nome da coluna alvo.
* 2: Tipo de dados.
* 3: Se o campo pode ser nulo.


```python
data_schema = StructType( [ StructField('age',IntegerType(),True),
              StructField('name',StringType(),True)] )
```

Agora, vamos carregar novamente os dados mas dessa vez passando o nosso schema criado.


```python
df = spark.read.json('uploads/data/people.json', schema = data_schema )
```


```python
df.printSchema()
```

    root
     |-- age: integer (nullable = true)
     |-- name: string (nullable = true)
    

<br><br><br>
Link da Próxima Aula:<br>
[Aula 03 ](/spark/aula03_dataframe2.html) 

