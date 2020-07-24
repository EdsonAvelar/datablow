---
layout: post
title:  "Curso de Spark e Python: Aula 03 - Hello World"
categories: [ curso_spark ]
author: adriano
permalink: "/spark/aula03_dataframe2"
image: "https://penseemti.com.br/wp-content/uploads/2017/02/A-importa%CC%82ncia-do-Banco-de-Dados.jpg"
tag: [featured]
---

# Curso de Spark e Python
## Aula 03 - DataFrame II

Seja bem vindo a mais uma aula sobre Spark e Python. Hoje vamos continuar nossos estudos sobre Dataframe Spark. 

Vamos fazer o mesmo da aula passada. 
1. Primeiro utilizamos o findspark para pegar o pacote pyspark


```python
import findspark
findspark.init()

```

2. Importamos o SparkSession e criamos uma Sessão Spark


```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
```

3. Criamos nosso Schema para controlar a importação os dados


```python
from pyspark.sql.types import (StructField, StringType,
                              IntegerType, StructType)

data_schema = StructType( [ StructField('age',IntegerType(),True),
              StructField('name',StringType(),True)] )
```

4. Importamos os dados e criamos um DataFrame


```python
df = spark.read.json('uploads/data/people.json', schema = data_schema )
```

5. Finalmente imprimimos o Schema para saber se está tudo certo.

df.printSchema()

## Selecionando os dados (select)
Vamos analisar os dois comandos abaixo. Ambos tentam pegar a coluna 'age', porém o primeiro utiliza um sintaxe semelhante ao `pandas` e o segundo utiliza a função select. No primeiro caso o retorno é um objeto Column. 

```python
type(df['age'] )
```




    pyspark.sql.column.Column



O problema é que não é possível manipular um objeto `Column`. Ele não foi feito para isso. Veja o comando abaixo.


```python
df['age'].show()
```


    ---------------------------------------------------------------------------

    TypeError                                 Traceback (most recent call last)

    <ipython-input-15-176e0fb11baa> in <module>
    ----> 1 df['age'].show()
    

    TypeError: 'Column' object is not callable


O DataFrame é o objeto ideal para manipulação. Para obter um DataFrame utiliza-se o comando Select.


```python
type(df.select('age') )
```




    pyspark.sql.dataframe.DataFrame



Agora sim é possível obter as informações, pois estamos trabalhando com um DataFrame.


```python
df.select('age').show()
```

    +----+
    | age|
    +----+
    |null|
    |  30|
    |  19|
    +----+
    


Para obter as primeiras linhas de um dataframe usa-se o comando `head`


```python
#Pega as duas primeiras linhas
df.head(2)
```




    [Row(age=None, name='Michael'), Row(age=30, name='Andy')]



Obtendo multiplas colunas


```python
df.select(['age','name']).show()
```

    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    


## Criando e Adicionando Novas Colunas (WithColumn)

A criação de novas colunas é uma das operações mais comuns no dia dia de quem trabalha com spark. Essa operação é realizada com a função `withColumn`


```python
df.withColumn('newage', df['age'] ).show()
```

    +----+-------+------+
    | age|   name|newage|
    +----+-------+------+
    |null|Michael|  null|
    |  30|   Andy|    30|
    |  19| Justin|    19|
    +----+-------+------+
    


É possível fazer transformações na coluna criada


```python
df.withColumn('double_age', df['age'] * 2).show()
```

    +----+-------+----------+
    | age|   name|double_age|
    +----+-------+----------+
    |null|Michael|      null|
    |  30|   Andy|        60|
    |  19| Justin|        38|
    +----+-------+----------+
    


A nova coluna pode ser uma expressão


```python
df.withColumn('jovem', df['age'] < 20).show()
```

    +----+-------+------+
    | age|   name|newage|
    +----+-------+------+
    |null|Michael|  null|
    |  30|   Andy| false|
    |  19| Justin|  true|
    +----+-------+------+
    


Mas lembre-se, um dataframe é imutável. ```df.show()``` mostrará o mesmo resultado de antes do `withColumn`


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
    


Não é possível acrescentar uma coluna ao dataframe original, mas podemos criar outro dataframe com a coluna adicionada.


```python
df1 = df.withColumn('jovem', df['age'] < 20)
df1.show()
```

    +----+-------+-----+
    | age|   name|jovem|
    +----+-------+-----+
    |null|Michael| null|
    |  30|   Andy|false|
    |  19| Justin| true|
    +----+-------+-----+
    


Outra operação comum é renomear colunas. 


```python
df.withColumnRenamed('age','age2').show()
```

    +----+-------+
    |age2|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    


## Usando SQL Puro

Com Spark Dataframe é possivel realizar operações como se tiveremos trabalhando com banco SQL. <br>

Primeiramente, vamos transformar nosso dataframe em um TempView.


```python
df.createOrReplaceTempView('people')
```

Vamos criar e executar a Query SQL, guardamos o resultado em `results`


```python
sql_query = "SELECT * FROM people"
results = spark.sql(sql_query )
```

Mostramos os resultados e voilá!


```python
results.show()
```

    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    


Outro exemplo:


```python
sql_query = "SELECT * FROM people WHERE age=30"
results = spark.sql(sql_query )
```


```python
results.show()
```

    +---+----+
    |age|name|
    +---+----+
    | 30|Andy|
    +---+----+
    


Em nosso curso o foco não será em operações SQL e sim em Python. <br>
Ficaremos por aqui.

[Aula 04]("/categories.html#curso_spark")
