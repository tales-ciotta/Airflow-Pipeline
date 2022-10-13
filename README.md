# Desafio Pipeline dados com Airflow
Esse repositório é parte de um desafio do programa Lighthouse da empresa Indicium tech. 

 



## Instruções do desafio

* Criar uma task que lê os dados da tabela 'Order' do banco de dados Northwind_small.sqlite. Essa task deve escrever um arquivo chamado "output_orders.csv".
* Criar uma task que lê os dados da tabela "OrderDetail" do mesmo banco de dados e juntar com o arquivo "output_orders.csv" que você exportou na tarefa anterior. Essa task deve calcular qual a soma da quantidade vendida (Quantity) com destino (ShipCity) para o Rio de Janeiro. Você deve exportar essa contagem em arquivo "count.txt" que contenha somente esse valor em formato texto.
* É necessário criar uma variável no Airflow com a key "my_email" e com seu endereço de e-mail no campo value.



## Requisitos
* Airflow instalado localmente
* Biblioteca pandas
* Biblioteca sqlite3
Cheque o arquivo requirements.txt para verificar todas bibliotecas utilizadas com mais detalhes.
## Criação das tasks

O código completo se encontra no arquivo elt_dag.py, mas as funções e operadores python criados podem ser observados separadamente nesta seção.

#### Para a primeira task, é necessário extrair a tabela 'Order' em um arquivo de formato csv. Para isso define-se a função:
```
def orders_to_csv():
        connect = sqlite3.connect('./Northwind_small.sqlite', isolation_level=None,
        detect_types=sqlite3.PARSE_COLNAMES)
        #Consulta-se no banco Northwind a tabela order:
        sql = """
            SELECT * FROM "Order"
        """
        db_df = pd.read_sql_query(sql, connect) #O dataframe recebe a query realizada
        db_df.to_csv('./output_orders.csv', index=False) #Tabela orders é exportada para csv

#Operador do Python para a função orders_to_csv

    export_orders_to_csv = PythonOperator(
        task_id='orders_to_csv',
        python_callable=orders_to_csv,
        provide_context=True
    )

``` 
#### Na segunda task há vários processos a serem feitos. É preciso realizar a extração de uma nova tabela, juntar diferentes fontes de dados (arquivo csv gerado na task anterior + resultado da nova consulta) e por fim realizar a soma da quantidade vendida com destino para o Rio de Janeiro. Define-se a função:
```
def count_to_txt():
        connect = sqlite3.connect('./Northwind_small.sqlite', isolation_level=None,
        detect_types=sqlite3.PARSE_COLNAMES)
        #Realiza-se a consulta para extrair a tabela OrderDetail
        sql = """
            SELECT * FROM OrderDetail
        """
        orderdetails_df = pd.read_sql_query(sql, connect)
        orders_df = pd.read_csv('./output_orders.csv') #Criando uma variável que recebe o arquivo csv gerado anteriormente
        df_join = pd.merge(orderdetails_df, orders_df, how='inner', left_on = 'OrderId', right_on = 'Id') #Realizando pelo pandas o join das duas fontes de dados
        df_query = df_join.query('(ShipCity == "Rio de Janeiro")')['Quantity'].sum() #Realiza-se a contagem através da query do pandas
        txt = open("count.txt", "a") #Cria-se o arquivo txt
        df_query_string=df_query.astype(str) #Para escrever no arquivo é necessário antes converter o dataframe para string, caso contrário o arquivo será gerado e um erro fará com que ele fique em branco
        txt.write(df_query_string) #Escreve no arquivo txt o resultado da count

    #Operador do Python para a função count_to_txt 

    export_count_to_txt = PythonOperator(
        task_id='count_to_txt',
        python_callable=count_to_txt,
        provide_context=True
    )
``` 

## Autor da resolução

- [Tales Rocha Ciotta](https://www.linkedin.com/in/talesciotta/)
