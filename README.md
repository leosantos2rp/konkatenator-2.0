## Sumário

- 1. Objetivo
- 2. Funcionamento
- 3. DAG

### 1. Objetivo
Visando melhor praticidade e eficiência, o **Konkatenator 2.0** foi criado para realizar automaticamente o *Concatenate* tanto de tabelas em formato ORC quanto em formato PARQUET. Este diretório contem o *.py* e a DAG responsáveis pela execução. 

### 2. Funcionamento
O arquivo **konkatenator_2.0.py** possui toda a estrutura necessária para concatenar. A execução do código é feita a partir da passagem dos seguintes argumentos como parâmetro:
1. user: usuário de serviço;
2. database: database onde será realizada a concatenação;
3. full: se **True**, executa em todas as partições. Se **False**, apenas nas 3 últimas. (Lembrando que a partição mais recente (geralmente a do dia atual) é sempre ignorada para evitar problemas).

O **konkatenator_2.0.py** funciona da seguinte forma:
1. Os argumentos citados acima são passados na execução da task e recebidos no código;
2. A partir do database é realizada uma *query* que retorna todas as tabelas do mesmo, rodando um loop a partir do resultado;
3. Para cada tabela, primeiro é verificado se a tabela é particionada ou não (só é possível concatenar tabelas particionadas). Se sim, busca todas as partições da tabela;
4. Condicional que verifica se o argumento full é **True** ou **False**. Como dito anteriormente, **True** concatena todas as partições da tabela, **False** só as 3 últimas;
5. Outro loop é instanciado para rodar as partições dessa tabela. Para cada partição, é montado o hdfsPath onde a mesma será concatenada;
6. A função *orcOrParquet()* é chamada. Essa função vai verificar se o formato da tabela é ORC ou PARQUET, como o nome sugere;
7. Assim que o formato é descoberto, é chamada a função *concatOrc()* ou a função *concatParquet()*, dependendo do formato. Ambas utilizam o *coalesce()* do Spark, que concatena as partições.

### 3. DAG
A DAG de concatenação contem 2 tasks principais:
1. *t_01_konkatenator*: a task principal do tipo *TwoRPSparkSubmitOperator()*, que de fato chama e roda o arquivo **konkatenator_2.0.py**. Dentre todos os argumentos, o mais importante é o *application_args*, onde são passados *user*, *database*, e *full* (**True** ou **False**);
2. *t_02_cleanerCheckpoint*: task do tipo *PythonOperator()*, que chama uma função python contendo duas subtasks do tipo *BashOperator*. Uma task roda o comando bash necessário para executar todo o código, a partir do *user*. A outra roda o comando bash que limpa todos os arquivos gerados pelo *checkpoint()* do spark. 

A DAG atua apenas em databases de RAW DATA. O correto é que a DAG seja executada apenas fora de horários de grande utilização do cluster.