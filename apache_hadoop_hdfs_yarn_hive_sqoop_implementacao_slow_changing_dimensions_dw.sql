== MELHOR VISUALIZADO NO SUBLIME TEXT ==

Engenharia de Dados com Apache Hadoop
Implementacao de Slow Changing Dimensions em um DW com Hive e Sqoop no Apache Hadoop
Gleidson Gomes - 29/10/2023

====================================================================================================================================================================================================
# Contextualizacao:

Implementacao de SCD (slow changing dimensions) tipo 1 e tipo 3 no hive a partir do hdfs em um data lake no apache hadoop. Os dados foram importados para o sgbd mysql oriundos 
do banco transacional 'adventureworks' da empresa ficticia Adventure Works. Os dados foram imputados no hdfs e hive com o sqoop. O resultado foi a modelagem dimensional para 
atender a perguntas de negocios referente ao processo de vendas.

# Resumo:

a) Precedentes:
- instalacao do mysql, hadoop (incluido hdfs e mapreduce), yarn, sqoop e hive;
- importacao do banco transacional 'adventureworks' para o mysql.

b) Passos do projeto:
- construcao da modelagem dimensional no formato 'star' a partir da base de dados oltp 'adventureworks';
- criacao da fato 'vendas';
- criacao das dimensoes 'tempo' e 'funcionario' com Slowly Changing Dimensions (SCD) tipo 1;
- criacao da dimensao 'produto' com com Slowly Changing Dimensions (SCD) tipo 3.

c) Perguntas de negocios:
- quantos produtos foram vendidos por ano, mes e dia por modelo, categoria e subcategoria;
- quais funcionarios obtiveram maior e menor volume de vendas mensais;
- detectar a sazonalidade e tendencia de vendas dos produtos por modelo, categoria e subcategoria;
- obter insights de negocios no contexto de vendas de produtos.

d) Trabalhos futuros:
- incluir novas visoes para incrementar as respostas para o negocio de vendas de produtos da empresa Adventure Works;
- utilizar um sgbd de mercado para gestão dos metadados do hive ao inves de utilizar o derby que e um repositorio de metadados local;
- realizar trabamento de dados para os campos com informacao nula ou vazia vindo do oltp;
- automatizar o processo de carga para periodicidade diaria;
- criar analises de dados com uma ferramenta de self service bi baseado na fato vendas.

====================================================================================================================================================================================================
# [1] Criacao do data lake no hdfs:

# Criacao dos diretorios para suportar o data lake
[hadoop@dataserver ~]$ hdfs dfs -mkdir /user
[hadoop@dataserver ~]$ hdfs dfs -mkdir /user/hadoop

# Verificacao da estrutura dos diretorios do data lake
[hadoop@dataserver ~]$ hdfs dfs -ls /user/hadoop/
[hadoop@dataserver ~]$

====================================================================================================================================================================================================
# [2] Criacao do data warehouse no hdfs:

# Criacao dos diretorios para suportar o data warehouse
[hadoop@dataserver ~]$ hdfs dfs -mkdir /user/hive
[hadoop@dataserver ~]$ hdfs dfs -mkdir /user/hive/warehouse

# Verificacao da estrutura dos diretorios do data lake
[hadoop@dataserver ~]$ hdfs dfs -ls /user/hive
Found 1 items
drwxr-xr-x   - hadoop supergroup          0 2023-08-13 15:18 /user/hive/warehouse

# Configurcao do data warehouse para MetaStore, repositorio de metadados do hive
[hadoop@dataserver ~]$ schematool -initSchema -dbType derby

====================================================================================================================================================================================================
# [3] Construção do data lake com apache hadoop e hive: 

# Criação de base de dados no hdfs pelo hive
hive> create database dwvendas;
OK

# Verificacao da criacao da base de dados
hive> show databases;
OK
default
dwvendas

hive> use dwvendas;
OK

# Verificacao da existencia de tabelas na nova base de dados
hive> show tables;
OK

# Verificacao da criacao do banco dentro do hdfs
[hadoop@dataserver ~]$ hdfs dfs -ls /user/hive/warehouse
Found 1 items
drwxr-xr-x   - hadoop supergroup          0 2023-11-03 11:20 /user/hive/warehouse/dwvendas.db
[hadoop@dataserver ~]$ 

====================================================================================================================================================================================================
# [4] Criacao e carga da tabela Funcionario (Employee):

# Verificacao dos registros a serem importados para o hive
mysql> SELECT EmployeeID AS cod_funcionario, Title as dsc_cargo, DATE_FORMAT(BirthDate, '%d/%m/%Y') as dat_aniversario, MaritalStatus as sgl_estado_civil, Gender as sgl_genero, DATE_FORMAT(SYSDATE(), '%d/%m/%Y %H:%i:%s') AS dat_carga, NULL AS dat_atualizacao FROM employee LIMIT 10;

+-----------------+------------------------------+-----------------+------------------+------------+---------------------+-----------------+
| cod_funcionario | dsc_cargo                    | dat_aniversario | sgl_estado_civil | sgl_genero | dat_carga           | dat_atualizacao |
+-----------------+------------------------------+-----------------+------------------+------------+---------------------+-----------------+
|               1 | Production Technician - WC60 | 15/05/1972      | M                | M          | 15/10/2023 19:22:44 |            NULL |
|               2 | Marketing Assistant          | 03/06/1977      | S                | M          | 15/10/2023 19:22:44 |            NULL |
|               3 | Engineering Manager          | 13/12/1964      | M                | M          | 15/10/2023 19:22:44 |            NULL |
|               4 | Senior Tool Designer         | 23/01/1965      | S                | M          | 15/10/2023 19:22:44 |            NULL |
|               5 | Tool Designer                | 29/08/1949      | M                | M          | 15/10/2023 19:22:44 |            NULL |
+-----------------+------------------------------+-----------------+------------------+------------+---------------------+-----------------+

# Verificacao do tipo de dados dos registros a serem importados para o hive
mysql> describe employee;

+------------------+---------------+------+-----+-------------------+-------------------+
| Field            | Type          | Null | Key | Default           | Extra             |
+------------------+---------------+------+-----+-------------------+-------------------+
| EmployeeID       | int           | NO   | PRI | NULL              |                   |
| NationalIDNumber | varchar(15)   | NO   |     | NULL              |                   |
| ContactID        | int           | NO   |     | NULL              |                   |
| LoginID          | varchar(256)  | NO   |     | NULL              |                   |
| ManagerID        | int           | YES  |     | NULL              |                   |
| Title            | varchar(50)   | NO   |     | NULL              |                   |
| BirthDate        | datetime      | NO   |     | NULL              |                   |
| MaritalStatus    | varchar(1)    | NO   |     | NULL              |                   |
| Gender           | varchar(1)    | NO   |     | NULL              |                   |
| HireDate         | datetime      | NO   |     | NULL              |                   |
| SalariedFlag     | bit(1)        | NO   |     | NULL              |                   |
| VacationHours    | smallint      | NO   |     | NULL              |                   |
| SickLeaveHours   | smallint      | NO   |     | NULL              |                   |
| CurrentFlag      | bit(1)        | NO   |     | NULL              |                   |
| rowguid          | varbinary(16) | NO   |     | NULL              |                   |
| ModifiedDate     | timestamp     | NO   |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+------------------+---------------+------+-----+-------------------+-------------------+


# Importacao tabela employee do mySql para o hdfs:
[hadoop@dataserver ~]$ sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?serverTimezone=UTC --username root --password S@l@da8203 --table employee --m 1


# Verificacao da importacao
[hadoop@dataserver ~]$ hdfs dfs -ls /user/hadoop/
Found 1 items
drwxr-xr-x   - hadoop supergroup          0 2023-10-24 13:14 /user/hadoop/employee
[hadoop@dataserver ~]$ 


# Transferencia da tabela employee do hdfs para o diretorio local do hadoop com novo nome:
[hadoop@dataserver output]$ hdfs dfs -get /user/hadoop/employee/part-m-00000 /home/hadoop/output/employee-part-m-00000


# Verificacao da transferencia
[hadoop@dataserver output]$ ls -la
total 228
drwxr-xr-x.  2 hadoop hadoop     96 Oct 24 19:08 .
drwx------. 20 hadoop hadoop   4096 Oct 24 17:40 ..
-rw-r--r--.  1 hadoop hadoop  59711 Oct 24 18:59 employee-part-m-00000
[hadoop@dataserver output]$ 


# Execucao das configuracoes no hive
hive> SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
hive> SET hive.support.concurrency=true;
hive> SET hive.enforce.bucketing=true;
hive> SET hive.exec.dynamic.partition.mode=nonstrict;
hive> SET hive.compactor.initiator.on=true;
hive> SET hive.compactor.worker.threads=1;
hive> SET hive.auto.convert.join=false;


# Criacao de tabela externa e temporaria
hive> CREATE EXTERNAL TABLE stg_employee (
  employeeid            int,                                       
  nationalidnumber      varchar(256),                               
  contactid             int,                                       
  loginid               varchar(256),
  managerid             int,                                       
  title                 varchar(256),                               
  birthdate             varchar(256),                              
  maritalstatus         varchar(1),                                
  gender                varchar(1),                                
  hiredate              varchar(256),                              
  salariedflag          varchar(1),
  vacationhours         int,                                       
  sickleavehours        int,                                       
  currentflag           varchar(1),                                
  rowguid               varchar(256),                             
  modifieddate          varchar(256)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/home/hadoop/output/employee-part-m-00000';


# Carga de dados na tabela externa temporaria
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/employee-part-m-00000' OVERWRITE INTO TABLE stg_employee;


# Criacao da dimensao como tabela interna no formato ORC pelo hive com Slowly Changing Dimensions (SCD) tipo 1
hive> CREATE TABLE dim_funcionario (
  sk_funcionario      int, 
  cod_funcionario     int, 
  dsc_cargo           varchar(256),                               
  dat_aniversario     date,                              
  sgl_estado_civil    varchar(1),                                
  sgl_genero          varchar(1),
  dat_carga           timestamp, 
  dat_atualizacao     timestamp
)
CLUSTERED BY (sk_funcionario) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES('transactional'='true');    #criado para permitir AICD
--STORED AS ORC;


# Remocao dos registros para atualizacao
hive> TRUNCATE TABLE temp_funcionario;


# Insercao de dados com Slowly Changing Dimensions (SCD) tipo 1
# Atualizacao dos registros alterados 
hive> MERGE INTO dim_funcionario AS x
USING (SELECT employeeid, title, cast(to_date(from_unixtime(unix_timestamp(birthdate, 'yyyy-mm-dd'))) as date) AS dat_aniversario, maritalstatus, gender FROM stg_employee) y 
ON x.cod_funcionario = y.employeeid AND (x.dsc_cargo != y.title OR x.dat_aniversario != y.dat_aniversario OR x.sgl_estado_civil != y.maritalstatus OR x.sgl_genero != y.gender) 
WHEN MATCHED THEN UPDATE SET 
dsc_cargo = y.title,
dat_aniversario = y.dat_aniversario,
sgl_estado_civil = y.maritalstatus,
sgl_genero = y.gender,
dat_atualizacao = from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss'));


# Insercao/Atualizacao de dados com Slowly Changing Dimensions (SCD) tipo 1
--CREATE TABLE temp_funcionario AS
hive> INSERT INTO temp_funcionario
SELECT cod_funcionario, dsc_cargo, cast(to_date(from_unixtime(unix_timestamp(dat_aniversario, 'yyyy-mm-dd'))) as date) AS dat_aniversario, sgl_estado_civil, 
sgl_genero, from_unixtime(to_unix_timestamp(dat_carga,'yyyy/MM/dd HH:mm:ss')) AS dat_carga, from_unixtime(to_unix_timestamp(dat_atualizacao,'yyyy/MM/dd HH:mm:ss')) AS dat_atualizacao
FROM dim_funcionario 
UNION ALL 
SELECT employeeid, title, cast(to_date(from_unixtime(unix_timestamp(birthdate, 'yyyy-mm-dd'))) as date) AS dat_aniversario, maritalstatus, gender,
from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')) AS dat_carga, NULL AS dat_atualizacao FROM stg_employee
WHERE employeeid NOT IN (SELECT cod_funcionario FROM dim_funcionario); 


# Remocao dos registros para atualizacao
hive> TRUNCATE TABLE dim_funcionario;
hive> TRUNCATE TABLE carga_funcionario;
OK
hive>


# Criacao de tabela temporaria 
--hive> CREATE TABLE carga_funcionario AS
hive> INSERT INTO carga_funcionario
SELECT 
ROW_NUMBER() OVER () AS id, 
cod_funcionario, 
dsc_cargo, 
dat_aniversario, 
sgl_estado_civil, 
sgl_genero,
dat_carga, 
dat_atualizacao 
FROM temp_funcionario;
OK
hive>


# Insercao de dados da tabela externa para a interna
hive> INSERT INTO TABLE dim_funcionario
SELECT 
id,
cod_funcionario, 
dsc_cargo, 
dat_aniversario, 
sgl_estado_civil, 
sgl_genero, 
dat_carga, 
dat_atualizacao 
FROM carga_funcionario;


# Insercao de valores curingas
INSERT INTO TABLE dim_funcionario
SELECT 
-1,
-1, 
'Não se aplica', 
NULL, 
NULL, 
NULL, 
from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')) AS dat_carga, 
NULL AS dat_atualizacao;


# Insercao de valores curingas
INSERT INTO TABLE dim_funcionario
SELECT 
-2,
-2, 
'Não Cadastrado',
NULL, 
NULL, 
NULL, 
from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')) AS dat_carga, 
NULL AS dat_atualizacao;


# Verificacao das tabelas criadas pelo hive no hdfs
hive> show tables;
OK
carga_funcionario
dim_funcionario
stg_employee
temp_funcionario
hive> 


# Evidencias:
hive> select * from dim_funcionario limit 6;
OK
290 1   Production Technician - WC60  1972-01-14  M M 2023-11-04 02:00:10 NULL
144 147 Production Technician - WC20  1967-01-05  S F 2023-11-04 02:00:10 NULL
280 11  Design Engineer               1949-01-10  M M 2023-11-04 02:00:10 NULL
142 149 Application Specialist        1978-01-13  S M 2023-11-04 02:00:10 NULL
216 75  Production Technician - WC45  1975-01-25  M M 2023-11-04 02:00:10 NULL
140 151 Production Technician - WC20  1977-01-22  M F 2023-11-04 02:00:10 NULL
Time taken: 0.137 seconds, Fetched: 6 row(s)
hive> 


# Verificacao do funcionamento do Slowly Changing Dimensions (SCD) tipo 1
# Selecao do funcionario 11 aleatorio:
hive> select * from dim_funcionario where cod_funcionario = 11;
OK
280 11  Design Engineer 1949-01-10  M M 2023-11-04 02:00:10 NULL
Time taken: 0.171 seconds, Fetched: 1 row(s)


# Alteracao manualmente do cargo do funcionario para 'ANALISTA DE SISTEMAS' no arquivo de entrada para simular alteracao no registro no oltp
# Carga de dados na tabela externa temporaria
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/employee-part-m-00000' OVERWRITE INTO TABLE stg_employee;
OK
hive>

# Realizacao de nova carga de dados com os scripts acima.

# Verificacao do efeito Slowly Changing Dimensions (SCD) tipo 1. Para esta abordagem, nao temos o historico da alteracao, apenas a ultima data de atualizacao
hive> select * from dim_funcionario where cod_funcionario = 11;
OK
1 11  ANALISTA DE SISTEMAS  1949-01-10  M M 2023-11-04 02:00:10 2023-11-04 02:17:38
Time taken: 0.207 seconds, Fetched: 1 row(s)
hive> 

====================================================================================================================================================================================================

# [5] Criacao e carga da tabela Produto (Product):

# Verificacao dos registros a serem importados para o hive
mysql> SELECT a.ProductID AS cod_produto, a.Name as dsc_produto, d.Name as dsc_modelo_produto, c.Name as dsc_categoria_produto, b.Name as dsc_sub_categoria_produto, DATE_FORMAT(SYSDATE(), '%d/%m/%Y %H:%i:%s') AS dat_inicio_vigencia, NULL AS dat_fim_vigencia, 1 AS ind_vigencia, DATE_FORMAT(SYSDATE(), '%d/%m/%Y %H:%i:%s') AS dat_carga, NULL AS dat_atualizacao FROM product a INNER JOIN productmodel d ON a.ProductModelID = d.ProductModelID INNER JOIN productsubcategory b ON a.ProductSubcategoryID = b.ProductSubcategoryID INNER JOIN productcategory c ON b.ProductCategoryID = c.ProductCategoryID LIMIT 10;

+-------------+----------------------------+-------------------------+-----------------------+---------------------------+---------------------+------------------+--------------+---------------------+-----------------+
| cod_produto | dsc_produto                | dsc_modelo_produto      | dsc_categoria_produto | dsc_sub_categoria_produto | dat_inicio_vigencia | dat_fim_vigencia | ind_vigencia | dat_carga           | dat_atualizacao |
+-------------+----------------------------+-------------------------+-----------------------+---------------------------+---------------------+------------------+--------------+---------------------+-----------------+
|         680 | HL Road Frame - Black, 58  | HL Road Frame           | Components            | Road Frames               | 15/10/2023 19:21:04 |             NULL |            1 | 15/10/2023 19:21:04 |            NULL |
|         706 | HL Road Frame - Red, 58    | HL Road Frame           | Components            | Road Frames               | 15/10/2023 19:21:04 |             NULL |            1 | 15/10/2023 19:21:04 |            NULL |
|         707 | Sport-100 Helmet, Red      | Sport-100               | Accessories           | Helmets                   | 15/10/2023 19:21:04 |             NULL |            1 | 15/10/2023 19:21:04 |            NULL |
|         708 | Sport-100 Helmet, Black    | Sport-100               | Accessories           | Helmets                   | 15/10/2023 19:21:04 |             NULL |            1 | 15/10/2023 19:21:04 |            NULL |
|         714 | Long-Sleeve Logo Jersey, M | Long-Sleeve Logo Jersey | Clothing              | Jerseys                   | 15/10/2023 19:21:04 |             NULL |            1 | 15/10/2023 19:21:04 |            NULL |
+-------------+----------------------------+-------------------------+-----------------------+---------------------------+---------------------+------------------+--------------+---------------------+-----------------+


# Verificacao do tipo de dados dos registros a serem importados para o hive
mysql> describe product;

+-----------------------+---------------+------+-----+-------------------+-------------------+
| Field                 | Type          | Null | Key | Default           | Extra             |
+-----------------------+---------------+------+-----+-------------------+-------------------+
| ProductID             | int           | NO   | PRI | NULL              | auto_increment    |
| Name                  | varchar(50)   | NO   |     | NULL              |                   |
| ProductNumber         | varchar(25)   | NO   |     | NULL              |                   |
| MakeFlag              | bit(1)        | NO   |     | NULL              |                   |
| FinishedGoodsFlag     | bit(1)        | NO   |     | NULL              |                   |
| Color                 | varchar(15)   | YES  |     | NULL              |                   |
| SafetyStockLevel      | smallint      | NO   |     | NULL              |                   |
| ReorderPoint          | smallint      | NO   |     | NULL              |                   |
| StandardCost          | double        | NO   |     | NULL              |                   |
| ListPrice             | double        | NO   |     | NULL              |                   |
| Size                  | varchar(5)    | YES  |     | NULL              |                   |
| SizeUnitMeasureCode   | varchar(3)    | YES  |     | NULL              |                   |
| WeightUnitMeasureCode | varchar(3)    | YES  |     | NULL              |                   |
| Weight                | decimal(8,2)  | YES  |     | NULL              |                   |
| DaysToManufacture     | int           | NO   |     | NULL              |                   |
| ProductLine           | varchar(2)    | YES  |     | NULL              |                   |
| Class                 | varchar(2)    | YES  |     | NULL              |                   |
| Style                 | varchar(2)    | YES  |     | NULL              |                   |
| ProductSubcategoryID  | int           | YES  |     | NULL              |                   |
| ProductModelID        | int           | YES  |     | NULL              |                   |
| SellStartDate         | datetime      | NO   |     | NULL              |                   |
| SellEndDate           | datetime      | YES  |     | NULL              |                   |
| DiscontinuedDate      | datetime      | YES  |     | NULL              |                   |
| rowguid               | varbinary(16) | NO   |     | NULL              |                   |
| ModifiedDate          | timestamp     | NO   |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+-----------------------+---------------+------+-----+-------------------+-------------------+


mysql> describe productmodel;

+--------------------+---------------+------+-----+-------------------+-------------------+
| Field              | Type          | Null | Key | Default           | Extra             |
+--------------------+---------------+------+-----+-------------------+-------------------+
| ProductModelID     | int           | NO   | PRI | NULL              | auto_increment    |
| Name               | varchar(50)   | NO   |     | NULL              |                   |
| CatalogDescription | text          | YES  |     | NULL              |                   |
| Instructions       | text          | YES  |     | NULL              |                   |
| rowguid            | varbinary(16) | NO   |     | NULL              |                   |
| ModifiedDate       | timestamp     | NO   |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+--------------------+---------------+------+-----+-------------------+-------------------+


mysql> describe productsubcategory;

+----------------------+---------------+------+-----+-------------------+-------------------+
| Field                | Type          | Null | Key | Default           | Extra             |
+----------------------+---------------+------+-----+-------------------+-------------------+
| ProductSubcategoryID | int           | NO   | PRI | NULL              | auto_increment    |
| ProductCategoryID    | int           | NO   |     | NULL              |                   |
| Name                 | varchar(50)   | NO   |     | NULL              |                   |
| rowguid              | varbinary(16) | NO   |     | NULL              |                   |
| ModifiedDate         | timestamp     | NO   |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+----------------------+---------------+------+-----+-------------------+-------------------+


mysql> describe productcategory;

+-------------------+---------------+------+-----+-------------------+-------------------+
| Field             | Type          | Null | Key | Default           | Extra             |
+-------------------+---------------+------+-----+-------------------+-------------------+
| ProductCategoryID | int           | NO   | PRI | NULL              | auto_increment    |
| Name              | varchar(50)   | NO   |     | NULL              |                   |
| rowguid           | varbinary(16) | NO   |     | NULL              |                   |
| ModifiedDate      | timestamp     | NO   |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+-------------------+---------------+------+-----+-------------------+-------------------+


# Importacao tabela product do mySql para o hdfs
[hadoop@dataserver ~]$ sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?serverTimezone=UTC --username root --password S@l@da8203 --table product --m 1

# Importacao tabela productmodel do mySql para o hdfs
[hadoop@dataserver ~]$ sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?serverTimezone=UTC --username root --password S@l@da8203 --table productmodel --m 1

# Importacao tabela productsubcategory do mySql para o hdfs 
[hadoop@dataserver ~]$ sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?serverTimezone=UTC --username root --password S@l@da8203 --table productsubcategory --m 1

# Importacao tabela productcategory do mySql para o hdfs
[hadoop@dataserver ~]$ sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?serverTimezone=UTC --username root --password S@l@da8203 --table productcategory --m 1


# Verificacao da importacao
[hadoop@dataserver ~]$ hdfs dfs -ls /user/hadoop/
Found 5 items
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:35 /user/hadoop/employee
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:41 /user/hadoop/product
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:45 /user/hadoop/productcategory
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:43 /user/hadoop/productmodel
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:44 /user/hadoop/productsubcategory
[hadoop@dataserver ~]$ 


# Transferencia da tabela employee do hdfs para o diretorio local do hadoop com novo nome
[hadoop@dataserver output]$ hdfs dfs -get /user/hadoop/product/part-m-00000 /home/hadoop/output/product-part-m-00000
[hadoop@dataserver output]$ hdfs dfs -get /user/hadoop/productmodel/part-m-00000 /home/hadoop/output/productmodel-part-m-00000
[hadoop@dataserver output]$ hdfs dfs -get /user/hadoop/productsubcategory/part-m-00000 /home/hadoop/output/productsubcategory-part-m-00000
[hadoop@dataserver output]$ hdfs dfs -get /user/hadoop/productcategory/part-m-00000 /home/hadoop/output/productcategory-part-m-00000


# Verificacao da transferencia
[hadoop@dataserver output]$ ls -la
total 236
drwxr-xr-x.  2 hadoop hadoop    171 Oct 24 19:33 .
drwx------. 20 hadoop hadoop   4096 Oct 24 17:40 ..
-rw-r--r--.  1 hadoop hadoop  59711 Oct 24 18:59 employee-part-m-00000
-rw-r--r--.  1 hadoop hadoop    326 Oct 24 19:33 productcategory-part-m-00000
-rw-r--r--.  1 hadoop hadoop  54790 Oct 24 19:07 productmodel-part-m-00000
-rw-r--r--.  1 hadoop hadoop 108165 Oct 24 19:08 product-part-m-00000
-rw-r--r--.  1 hadoop hadoop   3132 Oct 24 19:33 productsubcategory-part-m-00000
[hadoop@dataserver output]$


# Criacao de tabela externa e temporaria
hive> CREATE EXTERNAL TABLE stg_product (
ProductID             int, 
Name                  varchar(256), 
ProductNumber         varchar(256), 
MakeFlag              varchar(2), 
FinishedGoodsFlag     varchar(2),   
Color                 varchar(256), 
SafetyStockLevel      int, 
ReorderPoint          int, 
StandardCost          varchar(256), 
ListPrice             varchar(256), 
Size                  varchar(256), 
SizeUnitMeasureCode   varchar(256), 
WeightUnitMeasureCode varchar(256), 
Weight                varchar(256), 
DaysToManufacture     int,  
ProductLine           varchar(2), 
Class                 varchar(2), 
Style                 varchar(2), 
ProductSubcategoryID  int, 
ProductModelID        int, 
SellStartDate         varchar(256),
SellEndDate           varchar(256),
DiscontinuedDate      varchar(256),
rowguid               varchar(256),
ModifiedDate          varchar(256)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/home/hadoop/output/product-part-m-00000';

# Carga de dados na tabela externa temporaria
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/product-part-m-00000' OVERWRITE INTO TABLE stg_product;


# Criacao de tabela externa e temporaria
hive> CREATE EXTERNAL TABLE stg_productmodel (
ProductModelID       int,
Name                 varchar(256),
CatalogDescription   varchar(256),
Instructions         varchar(256),
rowguid              varchar(256),
ModifiedDate         varchar(256)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/home/hadoop/output/productmodel-part-m-00000';
OK
hive>

# Carga de dados na tabela externa temporaria
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/productmodel-part-m-00000' OVERWRITE INTO TABLE stg_productmodel;
OK
hive>


# Criacao de tabela externa e temporaria
hive> CREATE EXTERNAL TABLE stg_productsubcategory (
ProductSubcategoryID   int,
ProductCategoryID      int,
Name                   varchar(256),
rowguid                varchar(256),
ModifiedDate           varchar(256)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/home/hadoop/output/productsubcategory-part-m-00000';
OK
hive>

# Carga de dados na tabela externa temporaria
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/productsubcategory-part-m-00000' OVERWRITE INTO TABLE stg_productsubcategory;
OK
hive>


# Criacao de tabela externa
hive> CREATE EXTERNAL TABLE stg_productcategory (
ProductCategoryID   int,
Name                varchar(256),
rowguid             varchar(256),
ModifiedDate        varchar(256)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/home/hadoop/output/productcategory-part-m-00000';
OK
hive>

# Carga de dados na tabela externa temporaria
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/productcategory-part-m-00000' OVERWRITE INTO TABLE stg_productcategory;
OK
hive>


# Carga de dados na tabela externa temporaria
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/product-part-m-00000' OVERWRITE INTO TABLE stg_product;
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/productmodel-part-m-00000' OVERWRITE INTO TABLE stg_productmodel;
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/productsubcategory-part-m-00000' OVERWRITE INTO TABLE stg_productsubcategory;
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/productcategory-part-m-00000' OVERWRITE INTO TABLE stg_productcategory; 
OK
hive>


# Criacao da dimensao como tabela interna no formato ORC pelo hive com Slowly Changing Dimensions (SCD) tipo 3
hive> CREATE TABLE dim_produto (
sk_produto                int, 
cod_produto               int, 
dsc_produto               varchar(256), 
dsc_modelo_produto        varchar(256), 
dsc_categoria_produto     varchar(256),  
dsc_sub_categoria_produto varchar(256), 
dat_inicio_vigencia       timestamp, 
dat_fim_vigencia          timestamp, 
ind_vigencia              int, 
dat_carga                 timestamp, 
dat_atualizacao           timestamp
)
CLUSTERED BY (sk_produto) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES('transactional'='true');    #criado para permitir AICD
OK
hive>


# Insercao de dados com Slowly Changing Dimensions (SCD) tipo 3 - alteracao do registro alterado para inativo
hive> MERGE INTO dim_produto AS x
USING (SELECT a.ProductID AS cod_produto, a.Name AS dsc_produto, d.Name AS dsc_modelo_produto, c.Name AS dsc_categoria_produto, b.Name AS dsc_sub_categoria_produto 
FROM stg_product a 
LEFT JOIN stg_productmodel d ON a.ProductModelID = d.ProductModelID 
LEFT JOIN stg_productsubcategory b ON a.ProductSubcategoryID = b.ProductSubcategoryID 
LEFT JOIN stg_productcategory c ON b.ProductCategoryID = c.ProductCategoryID) y 
ON x.cod_produto = y.cod_produto AND ind_vigencia = 1 AND 
(x.dsc_produto != y.dsc_produto OR x.dsc_modelo_produto != y.dsc_modelo_produto OR x.dsc_categoria_produto != y.dsc_categoria_produto OR x.dsc_sub_categoria_produto != y.dsc_sub_categoria_produto)
WHEN MATCHED THEN UPDATE SET 
dat_fim_vigencia = from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')), 
ind_vigencia = 0, 
dat_atualizacao = from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss'));
OK
hive>


# Remocao dos registros para atualizacao
hive> TRUNCATE TABLE carga_produto;


# Insercao de dados com Slowly Changing Dimensions (SCD) tipo 3 - selecao dos atuais, novos e modificados registros 
--CREATE TABLE carga_produto AS
hive> INSERT INTO carga_produto
SELECT cod_produto, dsc_produto, dsc_modelo_produto, dsc_categoria_produto, dsc_sub_categoria_produto, 
from_unixtime(to_unix_timestamp(dat_inicio_vigencia,'yyyy/MM/dd HH:mm:ss')) AS dat_inicio_vigencia, from_unixtime(to_unix_timestamp(dat_fim_vigencia,'yyyy/MM/dd HH:mm:ss')) AS dat_fim_vigencia, 
ind_vigencia, from_unixtime(to_unix_timestamp(dat_carga,'yyyy/MM/dd HH:mm:ss')) AS dat_carga, from_unixtime(to_unix_timestamp(dat_atualizacao,'yyyy/MM/dd HH:mm:ss')) AS dat_atualizacao FROM dim_produto 
UNION ALL 
SELECT a.ProductID AS cod_produto, a.Name AS dsc_produto, d.Name AS dsc_modelo_produto, c.Name AS dsc_categoria_produto, 
b.Name AS dsc_sub_categoria_produto, from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')) AS dat_inicio_vigencia, 
NULL AS dat_fim_vigencia, 1 AS ind_vigencia, from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')) AS dat_carga, NULL AS dat_atualizacao 
FROM stg_product a 
LEFT JOIN stg_productmodel d ON a.ProductModelID = d.ProductModelID 
LEFT JOIN stg_productsubcategory b ON a.ProductSubcategoryID = b.ProductSubcategoryID 
LEFT JOIN stg_productcategory c ON b.ProductCategoryID = c.ProductCategoryID
WHERE a.ProductID NOT IN (SELECT cod_produto FROM dim_produto) 
UNION ALL 
SELECT a.ProductID AS cod_produto, a.Name AS dsc_produto, d.Name AS dsc_modelo_produto, c.Name AS dsc_categoria_produto, 
b.Name AS dsc_sub_categoria_produto, from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')) AS dat_inicio_vigencia, 
NULL AS dat_fim_vigencia, 1 AS ind_vigencia, from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')) AS dat_carga, NULL AS dat_atualizacao
FROM stg_product a 
LEFT JOIN stg_productmodel d ON a.ProductModelID = d.ProductModelID 
LEFT JOIN stg_productsubcategory b ON a.ProductSubcategoryID = b.ProductSubcategoryID 
LEFT JOIN stg_productcategory c ON b.ProductCategoryID = c.ProductCategoryID
INNER JOIN (SELECT cod_produto, MAX(ind_vigencia) FROM dim_produto GROUP BY cod_produto HAVING MAX(ind_vigencia) = 0) e
ON a.ProductID = e.cod_produto;


# Remocao dos registros para atualizacao
hive> TRUNCATE TABLE dim_produto;
hive> TRUNCATE TABLE temp_produto;
OK
hive>


# Criacao de tabela temporaria 
--hive> CREATE TABLE temp_produto AS
hive> INSERT INTO temp_produto
SELECT  
ROW_NUMBER() OVER () AS id, 
cod_produto, 
dsc_produto, 
dsc_modelo_produto, 
dsc_categoria_produto, 
dsc_sub_categoria_produto, 
dat_inicio_vigencia, 
dat_fim_vigencia, 
ind_vigencia, 
dat_carga, 
dat_atualizacao 
FROM carga_produto;
OK
hive>


# Insercao de dados da tabela externa para a interna
hive> INSERT INTO TABLE dim_produto
SELECT 
id,
cod_produto, 
dsc_produto, 
dsc_modelo_produto, 
dsc_categoria_produto, 
dsc_sub_categoria_produto, 
dat_inicio_vigencia, 
dat_fim_vigencia, 
ind_vigencia, 
dat_carga, 
dat_atualizacao 
FROM temp_produto;
OK
hive>


# Insercao de valores curingas
hive> INSERT INTO TABLE dim_produto
SELECT 
-1,
-1, 
'Não se aplica', 
'Não se aplica', 
'Não se aplica', 
'Não se aplica', 
NULL, 
NULL, 
NULL, 
from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')) AS dat_carga, 
NULL AS dat_atualizacao;
OK
hive>


# Insercao de valores curingas
hive> INSERT INTO TABLE dim_produto
SELECT 
-2,
-2, 
'Não Cadastrado', 
'Não Cadastrado', 
'Não Cadastrado', 
'Não Cadastrado', 
NULL, 
NULL, 
NULL, 
from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')) AS dat_carga, 
NULL AS dat_atualizacao;
OK
hive>


# Verificacao das tabelas criadas pelo hive no hdfs
hive> show tables;
OK
carga_produto
dim_produto
stg_product
stg_productcategory
stg_productmodel
stg_productsubcategory
temp_produto
Time taken: 0.022 seconds, Fetched: 8 row(s)
hive> 


# Evidencias:
hive> select * from dim_produto limit 6;
OK
82  829 Touring Rear Wheel      Touring Rear Wheel      Components  Wheels          2023-11-02 22:33:47 NULL  1 2023-11-02 22:33:47 NULL
40  935 LL Mountain Pedal LL    Mountain Pedal          Components  Pedals          2023-11-02 22:33:47 NULL  1 2023-11-02 22:33:47 NULL
76  810 HL Mountain Handlebars  HL Mountain Handlebars  Components  Handlebars      2023-11-02 22:33:47 NULL  1 2023-11-02 22:33:47 NULL
38  808 LL Mountain Handlebars  LL Mountain Handlebars  Components  Handlebars      2023-11-02 22:33:47 NULL  1 2023-11-02 22:33:47 NULL
60  922 Road Tire Tube          Road Tire Tube          Accessories Tires and Tubes 2023-11-02 22:33:47 NULL  1 2023-11-02 22:33:47 NULL
36  937 HL Mountain Pedal       HL Mountain Pedal       Components  Pedals          2023-11-02 22:33:47 NULL  1 2023-11-02 22:33:47 NULL
Time taken: 1.971 seconds, Fetched: 6 row(s)
hive> 


# Verificacao do funcionamento do Slowly Changing Dimensions (SCD) tipo 3
# Selecao do produto 712 aleatorio:
hive> select * from dim_produto where cod_produto = 712;
OK
81  712 AWC Logo Cap  Cycling Cap Clothing  Caps  2023-11-02 22:33:47 NULL  1 2023-11-02 22:33:47 NULL
Time taken: 0.116 seconds, Fetched: 1 row(s)
hive> 


# Alteracao manualmente da descricao do produto para 'GLEIDSON GOMES' no arquivo de entrada para simular alteracao no registro no oltp
# Carga de dados na tabela externa temporaria
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/product-part-m-00000' OVERWRITE INTO TABLE stg_product;
OK
hive>


# Realizacao de nova carga de dados com os scripts acima.


# Verificacao do efeito Slowly Changing Dimensions (SCD) tipo 3
hive> select * from dim_produto where cod_produto = 712;
OK
2 712 AWC Logo Cap          Cycling Cap Clothing  Caps  2023-11-02 22:33:47 2023-11-03 14:01:57 0 2023-11-02 22:33:47 2023-11-03 14:01:57
1 712 GLEIDSON GOMES        Cycling Cap Clothing  Caps  2023-11-03 14:04:53 NULL                1 2023-11-03 14:04:53 NULL
Time taken: 0.148 seconds, Fetched: 2 row(s)
hive> 

====================================================================================================================================================================================================

# [6] Criacao e carga da tabela Tempo:

# Verificacao dos registros a serem importados para o hive
mysql> SELECT DISTINCT DATE_FORMAT(OrderDate, '%d/%m/%Y') AS sk_tempo, DAY(OrderDate) AS num_dia, MONTH(OrderDate) AS num_mes, YEAR(OrderDate) AS num_ano, DAYNAME(OrderDate) AS dsc_dia, MONTHNAME(OrderDate) AS dsc_mes, QUARTER(OrderDate) AS num_trimestre, DATE_FORMAT(SYSDATE(), '%d/%m/%Y %H:%i:%s') AS dat_carga FROM purchaseorderheader LIMIT 10;

+--------------+---------+---------+---------+-----------+----------+---------------+---------------------+
| sk_tempo     | num_dia | num_mes | num_ano | dsc_dia   | dsc_mes  | num_trimestre | dat_carga           |
+--------------+---------+---------+---------+-----------+----------+---------------+---------------------+
| 17/05/2001   |      17 |       5 |    2001 | Thursday  | May      |             2 | 21/10/2023 17:14:47 |
| 31/05/2001   |      31 |       5 |    2001 | Thursday  | May      |             2 | 21/10/2023 17:14:47 |
| 14/01/2002   |      14 |       1 |    2002 | Monday    | January  |             1 | 21/10/2023 17:14:47 |
| 15/01/2002   |      15 |       1 |    2002 | Tuesday   | January  |             1 | 21/10/2023 17:14:47 |
| 08/02/2002   |       8 |       2 |    2002 | Friday    | February |             1 | 21/10/2023 17:14:47 |
| 16/02/2002   |      16 |       2 |    2002 | Saturday  | February |             1 | 21/10/2023 17:14:47 |
| 20/02/2002   |      20 |       2 |    2002 | Wednesday | February |             1 | 21/10/2023 17:14:47 |
| 24/02/2002   |      24 |       2 |    2002 | Sunday    | February |             1 | 21/10/2023 17:14:47 |
| 25/02/2002   |      25 |       2 |    2002 | Monday    | February |             1 | 21/10/2023 17:14:47 |
| 12/03/2002   |      12 |       3 |    2002 | Tuesday   | March    |             1 | 21/10/2023 17:14:47 |
+--------------+---------+---------+---------+-----------+----------+---------------+---------------------+


# Verificacao do tipo de dados dos registros a serem importados para o hive
mysql> describe purchaseorderheader;
+-----------------+----------+------+-----+---------+-------+
| Field           | Type     | Null | Key | Default | Extra |
+-----------------+----------+------+-----+---------+-------+
| PurchaseOrderID | int      | YES  |     | NULL    |       |
| RevisionNumber  | tinyint  | YES  |     | NULL    |       |
| Status          | tinyint  | YES  |     | NULL    |       |
| EmployeeID      | int      | YES  |     | NULL    |       |
| VendorID        | int      | YES  |     | NULL    |       |
| ShipMethodID    | int      | YES  |     | NULL    |       |
| OrderDate       | datetime | YES  |     | NULL    |       |
| ShipDate        | datetime | YES  |     | NULL    |       |
| SubTotal        | double   | YES  |     | NULL    |       |
| TaxAmt          | double   | YES  |     | NULL    |       |
| Freight         | double   | YES  |     | NULL    |       |
| TotalDue        | double   | YES  |     | NULL    |       |
| ModifiedDate    | datetime | YES  |     | NULL    |       |
+-----------------+----------+------+-----+---------+-------+
13 rows in set (0.01 sec)


+-----------------+----------+------+-----+---------+-------+
| Field           | Type     | Null | Key | Default | Extra |
+-----------------+----------+------+-----+---------+-------+
| sk_tempo        | int      | YES  |     | NULL    |       |
| num_dia         | int      | YES  |     | NULL    |       |
| num_mes         | int      | YES  |     | NULL    |       |
| num_ano         | int      | YES  |     | NULL    |       |
| dsc_dia         | varchar  | YES  |     | NULL    |       |
| dsc_mes         | varchar  | YES  |     | NULL    |       |
| num_trimestre   | int      | YES  |     | NULL    |       |
| dat_carga       | varchar  | YES  |     | NULL    |       |
+-----------------+----------+------+-----+---------+-------+


# Importacao tabela purchaseorderheader do mySql para o hdfs:  
[hadoop@dataserver ~]$ sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?serverTimezone=UTC --username root --password S@l@da8203 --table purchaseorderheader --m 1


# Verificacao da importacao 
[hadoop@dataserver ~]$ hdfs dfs -ls /user/hadoop/
Found 6 items
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:35 /user/hadoop/employee
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:41 /user/hadoop/product
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:45 /user/hadoop/productcategory
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:43 /user/hadoop/productmodel
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:44 /user/hadoop/productsubcategory
drwxr-xr-x   - hadoop supergroup          0 2023-10-21 17:24 /user/hadoop/purchaseorderdetail
[hadoop@dataserver ~]$ 


# Transferencia da tabela employee do hdfs para o diretorio local do hadoop com novo nome
[hadoop@dataserver output]$ hdfs dfs -get /user/hadoop/purchaseorderheader/part-m-00000 /home/hadoop/output/purchaseorderheader-part-m-00000


# Verificacao da transferencia
[hadoop@dataserver output]$ ls -la
total 700
drwxr-xr-x.  2 hadoop hadoop    211 Oct 29 19:46 .
drwx------. 20 hadoop hadoop   4096 Oct 29 16:04 ..
-rw-r--r--.  1 hadoop hadoop  59711 Oct 24 18:59 employee-part-m-00000
-rw-r--r--.  1 hadoop hadoop    326 Oct 24 19:33 productcategory-part-m-00000
-rw-r--r--.  1 hadoop hadoop  54790 Oct 24 19:07 productmodel-part-m-00000
-rw-r--r--.  1 hadoop hadoop 108165 Oct 24 19:08 product-part-m-00000
-rw-r--r--.  1 hadoop hadoop   3132 Oct 24 19:33 productsubcategory-part-m-00000
-rw-r--r--.  1 hadoop hadoop 474600 Oct 29 19:46 purchaseorderheader-part-m-00000
[hadoop@dataserver output]$ 


# Criacao de tabela externa e temporaria
hive> CREATE EXTERNAL TABLE stg_purchaseorderheader (
EmployeeID              int,
Freight                 double,
ModifiedDate            timestamp,
OrderDate               timestamp,
PurchaseOrderID         int,
RevisionNumber          tinyint,
ShipDate                timestamp,
ShipMethodID            int,
Status                  tinyint,
SubTotal                double,
TaxAmt                  double,
TotalDue                double,
VendorID                int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/home/hadoop/output/purchaseorderheader-part-m-00000';
OK
Time taken: 0.361 seconds
hive>

# Carga de dados na tabela externa temporaria
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/purchaseorderheader-part-m-00000' OVERWRITE INTO TABLE stg_purchaseorderheader;
Loading data to table dw_vendas.stg_purchaseorderheader
OK
Time taken: 0.64 seconds
hive> 


# Criacao da dimensao como tabela interna no formato ORC pelo hive com Slowly Changing Dimensions (SCD) tipo 1
hive> CREATE TABLE dim_tempo (
sk_tempo         date,        
num_dia          int,
num_mes          int,
num_ano          int,
num_trimestre    int,
dat_carga        timestamp
)
STORED AS ORC;
OK
Time taken: 0.094 seconds


# Insercao de dados da tabela externa para a interna
hive> INSERT INTO TABLE dim_tempo 
SELECT DISTINCT 
to_date(OrderDate) AS sk_tempo,
day(OrderDate) AS num_dia,
month(OrderDate) AS num_mes,
year(OrderDate) AS num_ano, 
quarter(OrderDate) AS num_trimestre, 
from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')) AS dat_carga
FROM stg_purchaseorderheader
WHERE to_date(OrderDate) NOT IN (SELECT sk_tempo FROM dim_tempo);
OK
Time taken: 35.14 seconds
hive> 


# Insercao de valores curingas
hive> INSERT INTO TABLE dim_tempo
SELECT 
to_date('9999-12-02'),
-2,
-2, 
-2,
-2,
from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss'));
OK
Time taken: 35.14 seconds
hive>


# Insercao de valores curingas
hive> INSERT INTO TABLE dim_tempo
SELECT 
to_date('9999-12-01'),
-1,
-1, 
-1,
-1, 
from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss'));
OK
Time taken: 35.14 seconds
hive>


# Verificacao das tabelas criadas pelo hive no hdfs
hive> show tables;
OK
carga_produto
dim_produto
dim_tempo
stg_product
stg_productcategory
stg_productmodel
stg_productsubcategory
stg_purchaseorderheader
temp_produto
Time taken: 0.026 seconds, Fetched: 10 row(s)
hive> 


# Verificacao do efeito Slowly Changing Dimensions (SCD) tipo 1
hive> select * from dim_tempo limit 10;
OK
2001-05-25  25  5 2001  2 2023-11-03 14:21:10
2001-06-08  8   6 2001  2 2023-11-03 14:21:10
2002-01-22  22  1 2002  1 2023-11-03 14:21:10
2002-01-23  23  1 2002  1 2023-11-03 14:21:10
2002-02-16  16  2 2002  1 2023-11-03 14:21:10
2002-02-24  24  2 2002  1 2023-11-03 14:21:10
2002-02-28  28  2 2002  1 2023-11-03 14:21:10
2002-03-04  4   3 2002  1 2023-11-03 14:21:10
2002-03-05  5   3 2002  1 2023-11-03 14:21:10
2002-03-20  20  3 2002  1 2023-11-03 14:21:10
Time taken: 0.173 seconds, Fetched: 10 row(s)
hive> 

====================================================================================================================================================================================================
# [7] Criacao e carga da fato vendas:

# Verificacao dos registros a serem importados para o hive
mysql> SELECT a.EmployeeID AS cod_funcionario, b.ProductID AS cod_produto, DATE_FORMAT(a.OrderDate, '%d/%m/%Y') AS dat_pedido, b.OrderQty AS qte_pedido, b.UnitPrice AS vlr_unitario, ROUND(a.SubTotal, 2) AS vlr_sub_total, ROUND(a.TaxAmt, 2) AS vlr_imposto, ROUND(a.Freight, 2) AS vlr_frete, ROUND(a.TotalDue, 2) AS vlr_total, DATE_FORMAT(SYSDATE(), '%d/%m/%Y %H:%i:%s') AS dat_carga, NULL AS dat_atualizacao FROM purchaseorderheader a INNER JOIN purchaseorderdetail b on a.PurchaseOrderID = b.PurchaseOrderID LIMIT 10;

+-----------------+-------------+------------+------------+--------------+---------------+-------------+-----------+-----------+---------------------+-----------------+
| cod_funcionario | cod_produto | dat_pedido | qte_pedido | vlr_unitario | vlr_sub_total | vlr_imposto | vlr_frete | vlr_total | dat_carga           | dat_atualizacao |
+-----------------+-------------+------------+------------+--------------+---------------+-------------+-----------+-----------+---------------------+-----------------+
|             244 |           1 | 17/05/2001 |          4 |        50.26 |        201.04 |       16.08 |      5.03 |    222.15 | 21/10/2023 16:47:02 |            NULL |
|             231 |         359 | 17/05/2001 |          3 |        45.12 |         272.1 |       21.77 |       6.8 |    300.67 | 21/10/2023 16:47:02 |            NULL |
|             231 |         360 | 17/05/2001 |          3 |      45.5805 |         272.1 |       21.77 |       6.8 |    300.67 | 21/10/2023 16:47:02 |            NULL |
|             241 |         530 | 17/05/2001 |        550 |       16.086 |        8847.3 |      707.78 |    221.18 |   9776.27 | 21/10/2023 16:47:02 |            NULL |
|             266 |           4 | 17/05/2001 |          3 |      57.0255 |        171.08 |       13.69 |      4.28 |    189.04 | 21/10/2023 16:47:02 |            NULL |
|             164 |         512 | 31/05/2001 |        550 |       37.086 |       20397.3 |     1631.78 |    509.93 |  22539.02 | 21/10/2023 16:47:02 |            NULL |
|             223 |         513 | 31/05/2001 |        550 |      26.5965 |      14628.08 |     1170.25 |     365.7 |  16164.02 | 21/10/2023 16:47:02 |            NULL |
|             233 |         317 | 31/05/2001 |        550 |      27.0585 |      58685.55 |     4694.84 |   1467.14 |  64847.53 | 21/10/2023 16:47:02 |            NULL |
|             233 |         318 | 31/05/2001 |        550 |       33.579 |      58685.55 |     4694.84 |   1467.14 |  64847.53 | 21/10/2023 16:47:02 |            NULL |
|             233 |         319 | 31/05/2001 |        550 |      46.0635 |      58685.55 |     4694.84 |   1467.14 |  64847.53 | 21/10/2023 16:47:02 |            NULL |
+-----------------+-------------+------------+------------+--------------+---------------+-------------+-----------+-----------+---------------------+-----------------+


# Verificacao do tipo de dados dos registros a serem importados para o hive
mysql> describe purchaseorderdetail;

+-----------------------+--------------+------+-----+-------------------+-------------------+
| Field                 | Type         | Null | Key | Default           | Extra             |
+-----------------------+--------------+------+-----+-------------------+-------------------+
| PurchaseOrderID       | int          | NO   | PRI | NULL              |                   |
| PurchaseOrderDetailID | int          | NO   | PRI | NULL              | auto_increment    |
| DueDate               | datetime     | NO   |     | NULL              |                   |
| OrderQty              | smallint     | NO   |     | NULL              |                   |
| ProductID             | int          | NO   |     | NULL              |                   |
| UnitPrice             | double       | NO   |     | NULL              |                   |
| LineTotal             | double       | NO   |     | NULL              |                   |
| ReceivedQty           | decimal(8,2) | NO   |     | NULL              |                   |
| RejectedQty           | decimal(8,2) | NO   |     | NULL              |                   |
| StockedQty            | decimal(9,2) | NO   |     | NULL              |                   |
| ModifiedDate          | timestamp    | NO   |     | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+-----------------------+--------------+------+-----+-------------------+-------------------+
11 rows in set (0.00 sec)


# Importacao tabela purchaseorderdetail do mySql para o hdfs:
sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?serverTimezone=UTC --username root --password S@l@da8203 --table purchaseorderdetail --m 1


# Verificacao da importacao 
[hadoop@dataserver ~]$ hdfs dfs -ls /user/hadoop/
Found 7 items
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:35 /user/hadoop/employee
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:41 /user/hadoop/product
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:45 /user/hadoop/productcategory
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:43 /user/hadoop/productmodel
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:44 /user/hadoop/productsubcategory
drwxr-xr-x   - hadoop supergroup          0 2023-10-21 17:24 /user/hadoop/purchaseorderdetail
drwxr-xr-x   - hadoop supergroup          0 2023-10-15 20:47 /user/hadoop/purchaseorderheader
[hadoop@dataserver ~]$ 


# Transferencia da tabela employee do hdfs para o diretorio local do hadoop com novo nome
[hadoop@dataserver output]$ hdfs dfs -get /user/hadoop/purchaseorderdetail/part-m-00000 /home/hadoop/output/purchaseorderdetail-part-m-00000


# Verificacao da transferencia
[hadoop@dataserver output]$ ls -la
total 1508
drwxr-xr-x.  2 hadoop hadoop    251 Oct 29 19:55 .
drwx------. 20 hadoop hadoop   4096 Oct 29 16:04 ..
-rw-r--r--.  1 hadoop hadoop  59711 Oct 24 18:59 employee-part-m-00000
-rw-r--r--.  1 hadoop hadoop    326 Oct 24 19:33 productcategory-part-m-00000
-rw-r--r--.  1 hadoop hadoop  54790 Oct 24 19:07 productmodel-part-m-00000
-rw-r--r--.  1 hadoop hadoop 108165 Oct 24 19:08 product-part-m-00000
-rw-r--r--.  1 hadoop hadoop   3132 Oct 24 19:33 productsubcategory-part-m-00000
-rw-r--r--.  1 hadoop hadoop 824362 Oct 29 19:55 purchaseorderdetail-part-m-00000
-rw-r--r--.  1 hadoop hadoop 474600 Oct 29 19:46 purchaseorderheader-part-m-00000
[hadoop@dataserver output]$ 


# Criacao de tabela externa e temporaria
hive> CREATE EXTERNAL TABLE stg_purchaseorderdetail (
DueDate                timestamp,
LineTotal              double,
ModifiedDate           timestamp,
OrderQty               smallint,
ProductID              int,
PurchaseOrderDetailID  int,
PurchaseOrderID        int,
ReceivedQty            decimal(8,2),
RejectedQty            decimal(8,2),
StockedQty             decimal(9,2),
UnitPrice              double
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/home/hadoop/output/purchaseorderdetail-part-m-00000';
OK
Time taken: 0.361 seconds
hive>


# Carga de dados na tabela externa temporaria
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/purchaseorderdetail-part-m-00000' OVERWRITE INTO TABLE stg_purchaseorderdetail;
Loading data to table dw_vendas.stg_purchaseorderheader
OK
Time taken: 0.64 seconds
hive> 


# Criacao da dimensao como tabela interna no formato ORC pelo hive com Slowly Changing Dimensions (SCD) tipo 1
hive> CREATE TABLE fato_vendas (
sk_funcionario    int,
sk_produto        int,
sk_dat_pedido     date,
qte_pedido        smallint,
vlr_unitario      double,
vlr_sub_total     double,
vlr_imposto       double,
vlr_frete         double,
vlr_total         double,
dat_carga         timestamp, 
dat_atualizacao   timestamp
)
STORED AS ORC;


# Remocao dos registros para atualizacao
hive> TRUNCATE TABLE fato_vendas;


# Insercao de dados da tabela externa para a interna
hive> INSERT INTO TABLE fato_vendas
SELECT 
NVL(c.sk_funcionario, -2) AS sk_funcionario,
NVL(b.sk_produto, -2) AS sk_produto,
NVL(sk_tempo, to_date('9999-12-02')) AS sk_dat_pedido,
a.qte_pedido,
a.vlr_unitario,
ROUND(a.vlr_sub_total, 2) AS vlr_sub_total,
ROUND(a.vlr_imposto, 2) AS vlr_imposto,
ROUND(a.vlr_frete, 2) AS vlr_frete,
ROUND(a.vlr_total, 2) AS vlr_total,
from_unixtime(to_unix_timestamp(current_timestamp(),'yyyy/MM/dd HH:mm:ss')) AS dat_carga,
NULL AS dat_atualizacao
FROM (SELECT a.EmployeeID, b.ProductID, to_date(a.OrderDate) AS dat_pedido, NVL(b.OrderQty, 0) AS qte_pedido, NVL(b.UnitPrice, 0) AS vlr_unitario, 
NVL(a.SubTotal, 0) AS vlr_sub_total, NVL(a.TaxAmt, 0) AS vlr_imposto, NVL(a.Freight, 0) AS vlr_frete, NVL(a.TotalDue, 0) AS vlr_total 
FROM stg_purchaseorderheader a INNER JOIN stg_purchaseorderdetail b on a.PurchaseOrderID = b.PurchaseOrderID) a
LEFT JOIN dim_produto b
ON a.ProductID = b.cod_produto 
LEFT JOIN dim_funcionario c
ON a.EmployeeID = c.cod_funcionario
LEFT JOIN dim_tempo d
ON a.dat_pedido = d.sk_tempo;


# Teste para verificar possiveis erros no processo de carga
hive> select  
a.sk_funcionario, c.sk_produto, a.sk_dat_pedido, c.cod_produto
from fato_vendas a
inner join dim_produto c
on a.sk_produto = c.sk_produto;
inner join dim_funcionario b
on a.sk_funcionario = b.sk_funcionario
where c.sk_produto = -2
OR b.sk_funcionario = -2;
Total MapReduce CPU Time Spent: 29 seconds 370 msec
OK
Time taken: 74.083 seconds
hive> 


# Verificacao das tabelas criadas pelo hive no hdfs
hive> show tables;
OK
carga_funcionario
carga_produto
dim_funcionario
dim_produto
dim_tempo
fato_vendas
stg_employee
stg_product
stg_productcategory
stg_productmodel
stg_productsubcategory
stg_purchaseorderdetail
stg_purchaseorderheader
temp_funcionario
temp_produto
Time taken: 0.024 seconds, Fetched: 15 row(s)
hive> 


# Evidencias:
hive> select * from fato_vendas limit 6;
OK
50  203 2001-05-16  550 16.086  8847.3    707.78  221.18  9776.27   2023-11-06 16:01:02 NULL
60  38  2001-05-16  3   45.12   272.1     21.77   6.8     300.67    2023-11-06 16:01:02 NULL
25  4   2001-05-16  3   57.0255 171.08    13.69   4.28    189.04    2023-11-06 16:01:02 NULL
60  39  2001-05-16  3   45.5805 272.1     21.77   6.8     300.67    2023-11-06 16:01:02 NULL
47  1   2001-05-16  4   50.26   201.04    16.08   5.03    222.15    2023-11-06 16:01:02 NULL
58  6   2001-05-30  550 27.0585 58685.55  4694.84 1467.14 64847.53  2023-11-06 16:01:02 NULL
Time taken: 0.14 seconds, Fetched: 6 row(s)
hive> 
 

# Verificacao do funcionamento do Slowly Changing Dimensions (SCD) tipo 3
# Selecao do produto 75 aleatorio:
hive> select a.sk_funcionario, c.sk_produto, a.sk_dat_pedido, c.cod_produto, c.dsc_produto, c.dat_inicio_vigencia,
c.dat_fim_vigencia, c.ind_vigencia
from fato_vendas a
inner join dim_produto c
on a.sk_produto = c.sk_produto
where c.cod_produto = 396;
OK
68  75  2004-09-02  396 Hex Nut 18
50  75  2004-07-27  396 Hex Nut 18
50  75  2004-07-24  396 Hex Nut 18
68  75  2004-07-09  396 Hex Nut 18
68  75  2004-07-05  396 Hex Nut 18
50  75  2004-05-22  396 Hex Nut 18
50  75  2004-05-17  396 Hex Nut 18
68  75  2004-04-30  396 Hex Nut 18
68  75  2004-04-25  396 Hex Nut 18
50  75  2004-03-08  396 Hex Nut 18
50  75  2004-02-29  396 Hex Nut 18
127 75  2004-02-15  396 Hex Nut 18
68  75  2004-02-09  396 Hex Nut 18
27  75  2003-12-30  396 Hex Nut 18
50  75  2003-12-08  396 Hex Nut 18
17  75  2003-10-10  396 Hex Nut 18
47  75  2003-10-05  396 Hex Nut 18
17  75  2003-06-14  396 Hex Nut 18
127 75  2003-03-16  396 Hex Nut 18
Time taken: 21.397 seconds, Fetched: 19 row(s)
hive>  

# Alteracao manualmente da descricao do produto de codigo 396 para 'GLEIDSON GOMES' no arquivo de entrada para simular alteracao no registro no oltp
# Carga de dados na tabela externa temporaria
hive> LOAD DATA LOCAL INPATH '/home/hadoop/output/product-part-m-00000' OVERWRITE INTO TABLE stg_product;
OK
hive>

# Realizacao de nova carga de dados com os scripts acima.

# Verificacao do efeito Slowly Changing Dimensions (SCD) tipo 3
OK
50  2 2004-07-24  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
68  2 2004-04-25  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
68  2 2004-02-09  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
27  2 2003-12-30  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
68  2 2004-09-02  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
68  2 2004-07-09  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
50  2 2004-02-29  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
127 2 2003-03-16  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
17  2 2003-06-14  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
50  2 2004-05-17  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
50  2 2004-03-08  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
50  2 2004-07-27  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
50  2 2003-12-08  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
47  2 2003-10-05  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
127 2 2004-02-15  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
68  2 2004-07-05  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
68  2 2004-04-30  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
17  2 2003-10-10  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
50  2 2004-05-22  396 GLEIDSON GOMES 2023-11-06 17:23:01 NULL                       1
47  5 2003-10-05  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
50  5 2004-05-22  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
68  5 2004-07-05  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
68  5 2004-09-02  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
17  5 2003-06-14  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
50  5 2004-07-24  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
127 5 2003-03-16  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
68  5 2004-04-30  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
50  5 2004-02-29  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
127 5 2004-02-15  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
68  5 2004-04-25  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
68  5 2004-02-09  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
50  5 2004-03-08  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
68  5 2004-07-09  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
17  5 2003-10-10  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
50  5 2003-12-08  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
27  5 2003-12-30  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
50  5 2004-07-27  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
50  5 2004-05-17  396 Hex Nut 18            2023-11-04 23:10:16 2023-11-06 17:20:06 0
Time taken: 20.844 seconds, Fetched: 38 row(s)
hive> 

====================================================================================================================================================================================================

# Fim

== MELHOR VISUALIZADO NO SUBLIME TEXT ==