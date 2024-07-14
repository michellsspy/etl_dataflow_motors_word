import psycopg2
import apache_beam as beam
import logging
import pandas as pd
from google.cloud import storage
from google.cloud import secretmanager
import os
import json
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(levelname)s - %(message)s")

class LoadLanding(beam.DoFn):
    def process(self, element):
        logging.info("Recuperando os dados da tabela controller")
        controler = pd.read_csv('controller/controler.csv')

        logging.info("Criando uma lista vazia para recuperar os nomes das tabelas")
        list_tables_names = []
        
        logging.info("Recuperando os parâmetros de conexão com o banco de dados Postgres")
        logging.info("Criando um client para o Secret Manager")
        client = secretmanager.SecretManagerServiceClient()

        logging.info("Montando o nome da secret com os dados da conexão")
        db_name = "projects/872130982957/secrets/credentials_db/versions/1"
        
        logging.info("Acessando o valor da secret")
        response = client.access_secret_version(name=db_name)

        logging.info("Decodificando o payload")
        secret_payload = response.payload.data.decode("UTF-8")
        
        logging.info("Convertendo payload JSON para dicionário")
        secret_dict = None
        try:
            secret_dict = json.loads(secret_payload)
        except json.JSONDecodeError as e:
            logging.error(f"Erro ao carregar JSON: {e}")
            logging.error(f"Conteúdo recebido: {secret_payload}")
            raise

        logging.info("Definindo as variáveis de ambiente para conexão com o banco")
        os.environ["DB_HOST"] = secret_dict["DB_HOST"] 
        os.environ["DB_PORT"] = secret_dict["DB_PORT"]
        os.environ["DB_NAME"] = secret_dict["DB_NAME"]
        os.environ["DB_USER"] = secret_dict["DB_USER"]
        os.environ["DB_PASSWORD"] = secret_dict["DB_PASSWORD"]
        
        logging.info("Recuperando as variáveis via variável de ambiente")
        USERNAME = os.getenv("DB_USER")
        PASSWORD = os.getenv("DB_PASSWORD")
        HOST = os.getenv("DB_HOST")
        PORT = os.getenv("DB_PORT")
        DATABASE = os.getenv("DB_NAME")
        
        logging.info("Criando uma conexão com o banco de dados")
        conn_params = {
            "user": USERNAME,
            "password": PASSWORD,
            "host": HOST,
            "port": PORT,
            "database": DATABASE,
        }
        
        logging.info("Recuperando lista de nomes das tabelas")
        list_names = list(controler.loc[:, 'table'])
        
        try:
            for table in list_names:
                logging.info(f"Recuperando os nomes das tabelas: {list_tables_names}")
                list_tables_names.append(table)

                logging.info('Recuperando dados da tabela: ' + table)
                target_bucket = str(controler.loc[controler['table'] == table, 'target_bucket'].iloc[0])
                target_folder_path = str(controler.loc[controler['table'] == table, 'target_folder_path'].iloc[0])
                
                logging.info(f"Conectando ao banco de dados: {DATABASE}")
                conn = psycopg2.connect(**conn_params)      
                
                logging.info("Criando o cursor")
                cursor = conn.cursor()
                
                logging.info(f"Montando a query de consulta da tabela: {table}")
                query = f"SELECT * FROM {table}" #LIMIT 10"
                
                logging.info(f"Executando a query de consulta da tabela: {table}")
                cursor.execute(query)
                
                logging.info("Recuperando as colunas da tabela")
                col_names = [desc[0] for desc in cursor.description]
                                    
                logging.info(f"Recuperando os dados da consulta da tabela: {table}")
                rows = cursor.fetchall()
                
                logging.info(f"Criando um dataframe da tabela: {table}")
                df_new = pd.DataFrame(rows, columns=col_names)

                logging.info(f'{table} {"=" * (80 - len(table))} {df_new.shape}')
                
                cursor.close()
                conn.close()
                
                logging.info("Convertendo df para String")
                df_new = df_new.astype(str)
                
                logging.info("Criando as variáveis do storage client")
                bucket_name = target_bucket
                path = f"{target_folder_path}/{table}.parquet"            
                client = storage.Client()
                bucket = client.get_bucket(bucket_name)
                blob = bucket.blob(path)
                logging.info(f"Criando as variáveis para o path do storage client: {blob}")
                
                logging.info(f"Passando Dataframe para String: {table}")
                df_new = df_new.astype(str)
                
                logging.info(f"Salva o DataFrame (Converte para parquet): {table}")
                blob.upload_from_string(df_new.to_parquet(index=False), content_type='application/octet-stream')
                
                logging.info(f'{table} {"=" * (80 - len(table))} {df_new.shape}')
                
        except psycopg2.Error as e:
            logging.info(f"Erro encontrado durante a conexão: {e}")
            print("\n")
            yield list_tables_names

        print("\n")
        yield list_tables_names
