1º Qual o objetivo do comando cache em Spark?

Devido a forma que o Lazy Evaluation do spark funciona podemos utilizar o cache para salvar resultados de operações que antes iram ser executadas somente no momento da ação.
Assim sendo o comando "Cache" permite salvemos o resultado de uma operação dentro do cache para que o mesmo possa ser chamado mais vezes assim reduzindo a quantidade de operações realizadas no código.

2ºO mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê? 

Apesar do fato do MapReduce rodar quantidades de dados maiores que o Spark, o mesmo precisa salvar e ler os dados em disco.
Por outro lado, o Spark roda os jobs na memória RAM, tornando assim sua execução bem mais rápida que a do MapReduce

3º Qual é a função do SparkContext?

O SparkContext é o parâmetro que passamos ao iniciar uma seção Spark. Nele atribuímos a quantidade de memória, recursos e processadores que serão utilizados na nossa aplicação Spark.
Alem disso, o SparkContext é o responsavel por realizar a comunicação de todos os nós do cluster alem de poder ser utilizado para a criação de RDDs.

4º Explique com suas palavras o que é Resilient Distributed Datasets (RDD).

RDDs são abstrações de dados que o Spark cria para armazenar seus valores. O RDDs (Resilient, Distributed, Datasets) são conjuntos de dados resilientes ou seja os dados são resistentes a falhas, são distribuídos, que ao longo dos nós do cluster ajuda na recuperação do arquivo caso haja falha.
Outra característica importante é que um RDD é imutável. Sendo assim para realizar uma transformação nele é necessário, realizar a transformação e salva-lo como um novo RDD com a mudança requeria, o antigo RDD então não sofreu mudança alguma.

5º GroupByKey é menos eficiente que ReduceByKey em grandes dataset. Por quê?

Pois devido a grande quantidade de informações, o GroupByKey cria uma quantidade de informações que não eram necessárias para serem trafegadas.  
Em contra partida o ReduceByKey ja sabe oque fazer, identifica os pares dos elementos e depois começar a realizar as comparações nas outras partições. 
O ReduceByKey também evita que haja um menor risco de sobrecarga .

6º Explique o que o código Scala abaixo faz.

Logo no começo na primeira linha, é criado um objeto do SparkContext, o RDD textFile. No mesmo é atribuo um arquivo de dentro do HDFS.
Na segunda linha ele cria um novo RDD chamado "count", ele ira receber o todas as palavras do RDD anteriormente criado.
A seguir ele vai mapear todas as palavras chaves e atribuir o valor 1 a elas.
O ReduceByKey vai classificar e realizar uma operação de contagem de ocorrência de cada uma das palavras.
Na ultima linha é salvo o resultado dessa contagem dentro de um arquivo de texto. O mesmo é salvo dentro do HDFS .
