from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
#É necessário importar mais bilbiotecas para a manipulação dos dados:
import pandas as pd
import sqlite3



# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['tales.ciotta@indicium.tech'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],

) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
    #Definir a função para exportar a tabela orders do nosso banco de dados para csv
    def orders_to_csv():
        connect = sqlite3.connect('./Northwind_small.sqlite', isolation_level=None,
        detect_types=sqlite3.PARSE_COLNAMES)
        #Consulta-se no banco Northwind a tabela order:
        sql = """
            SELECT * FROM "Order"
        """
        db_df = pd.read_sql_query(sql, connect)#O dataframe recebe a query realizada
        db_df.to_csv('./output_orders.csv', index=False)#Tabela orders é exportada para csv

    #Operador do Python para a função orders_to_csv
    export_orders_to_csv = PythonOperator(
        task_id='orders_to_csv',
        python_callable=orders_to_csv,
        provide_context=True
    )
    #Definir a função para realizar a query da tabela OrderDetail -> Ler o csv gerado pela orders_to_csv -> Juntar os dois dataframes -> Realizar contagem -> Criar count.txt -> transformar o df em string e escrever em count.txt
    def count_to_txt():
        connect = sqlite3.connect('./Northwind_small.sqlite', isolation_level=None,
        detect_types=sqlite3.PARSE_COLNAMES)
        sql = """
            SELECT * FROM OrderDetail
        """
        orderdetails_df = pd.read_sql_query(sql, connect)
        orders_df = pd.read_csv('./output_orders.csv') #Criando uma variável que recebe o arquivo csv gerado anteriormente 
        df_join = pd.merge(orderdetails_df, orders_df, how='inner', left_on = 'OrderId', right_on = 'Id') #Realizando pelo pandas o join das duas fontes de dados
        df_query = df_join.query('(ShipCity == "Rio de Janeiro")')['Quantity'].sum() #Realiza-se a contagem através da query do pandas
        txt = open("count.txt", "a") #Cria-se o arquivo txt
        df_query_string=df_query.astype(str) #Para escrever no arquivo é necessário antes converter o dataframe para string, caso contrário o arquivo será gerado e um erro fará com que ele fique em branco
        txt.write(df_query_string)#Escreve no arquivo txt o resultado da count
    #Operador do Python para a função count_to_txt    
    export_count_to_txt = PythonOperator(
        task_id='count_to_txt',
        python_callable=count_to_txt,
        provide_context=True
    )
    #Operador do Python para função export_final_answer
    ##PS: A função export_final_answer foi definida logo após de default args no início do código.
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

##Definindo a ordem de execução no Airflow. Primeiro gera-se o arquivo csv de orders, depois realiza-se a contagem e por fim o resultado final é exportado
export_orders_to_csv >> export_count_to_txt >> export_final_output
