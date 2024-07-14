import psycopg2
import apache_beam as beam
import logging
import pandas as pd
from google.cloud import storage
from google.cloud import secretmanager
import os
import json

class IngestTables(beam.DoFn):
    def process(self, element):
        
        # Recuperando os dados da tabela controller
        controler = pd.read_csv('controller/controler.csv')

        # Criando uma lista vazia para recuperar os nomes das tabelas
        list_tables_names = []
        
        # Recuperando os parammetros de conexão com o banco de dados Postgrees 
        # Criando um client
        client = secretmanager.SecretManagerServiceClient()

        # Montando o nome da secret com os dados da conexão
        db_name = "projects/872130982957/secrets/credentials_db/versions/1"
        
        # Acessar o valor da secret
        response = client.access_secret_version(name=db_name)

        # Decodificando o payload
        secret_payload = response.payload.data.decode("UTF-8")
        
        # O payload é um Json - Crinado um dict
        secret_dict = None
        try:
            secret_dict = json.loads(secret_payload)
        except json.JSONDecodeError as e:
            logging.error(f"Erro ao carregar JSON: {e}")
            logging.error(f"Conteúdo recebido: {secret_payload}")
            raise

        
        # Definindo as variáveis de ambiente para conexão com o banco    
        os.environ["DB_HOST"] = secret_dict["DB_HOST"] 
        os.environ["DB_PORT"] = secret_dict["DB_PORT"]
        os.environ["DB_NAME"] = secret_dict["DB_NAME"]
        os.environ["DB_USER"] = secret_dict["DB_USER"]
        os.environ["DB_PASSWORD"] = secret_dict["DB_PASSWORD"]
        
        # Recupera as variáveis via variável de ambiente
        USERNAME = os.getenv("DB_USER")
        PASSWORD = os.getenv("DB_PASSWORD")
        HOST = os.getenv("DB_HOST")
        PORT = os.getenv("DB_PORT")
        DATABASE = os.getenv("DB_NAME")
        
        # Criando uma conexão com o banco de dados 
        
        conn_params = {
            "user": USERNAME,
            "password": PASSWORD,
            "host": HOST,
            "port": PORT,
            "database": DATABASE,
        }
        
        list_names = list(controler.loc[:, 'table'])
        
        for table in list_names:
            # Recuperando os nomes das tabelas
            list_tables_names.append(table)
            
            logging.info(f"Recuperando os nomes das tabelas: {list_tables_names}")

            logging.info('Recuperando dados da tabela: ' + table)
            
            # recuperando valores da tabela controller  ''''''
            target_bucket = str(controler.loc[controler['table'] == table, 'target_bucket'].iloc[0])
            target_folder_path = str(controler.loc[controler['table'] == table, 'target_folder_path'].iloc[0])             
            
            # Criando uma conexão com o banco de dados 
            #try:
            logging.info(f"Conectando ao banco de dados: {DATABASE}")
            conn = psycopg2.connect(**conn_params)      
            
            # Criando o cursor
            cursor = conn.cursor()
            
            # Montando a query de consulta da tabela
            query = f"SELECT * FROM {table}"
            logging.info(f"Montando a query de consulta da tabela: {query}")
            
            logging.info(f"Executando a query de consulta da tabela: {table}")
            cursor.execute(query)
            
            # Recuperando as colunas da tabela
            col_names = [desc[0] for desc in cursor.description]
            logging.info(f"Recuperando as colunas da tabela: {col_names}")
                                   
            # Recuperando os dados da consulta
            logging.info(f"Recuperando os dados da consulta da tabela: {table}")
            rows = cursor.fetchall()
            
            # Criando um dataframe
            logging.info(f"Criando um dataframe da tabela: {table}")
            df_new = pd.DataFrame(rows, columns=col_names)

            logging.info(f'{table} {"=" * (80 - len(table))} {df_new.shape}')
            
            cursor.close()
            conn.close() 
            
            logging.info("Convertendo df para String")
            df_new = df_new.astype(str)
            
            # Criando as variáveis do storage client            
            bucket_name = target_bucket
            path = f"{target_folder_path}/{table}.parquet"            
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(path)
            logging.info(f"Criando as variáveis para o path do storage client: {blob}")
            
            # Verificando a existência do arquivo no path informado
            logging.info(f"Verificando a existência do arquivo no path informado: {blob}")
            if blob.exists():
                logging.info(f"Arquivo existe no path informado: {table}")                # Lê o arquivo existente no GCS e cria um DataFrame
                gcs_file_path = f"gs://{bucket_name}/{path}"
                df_old = pd.read_parquet(gcs_file_path)
                
                df_old = df_old.astype(str)
                
                print(f"\n {df_old.head()} \n")    
                
                # Recupera o nome da primeira coluna para alinhar o merge
                merge_column = df_new.columns[0]
                logging.info(f"Recupera o nome da primeira coluna para alinhar o merge: {merge_column}")
                
                # Realiza o merge
                logging.info(f"Realiza o merge: {table}")
                df_combined = pd.merge(df_old, df_new, on=merge_column, how='outer', suffixes=('_old', ''))
                
                # Preenche os valores NaN nos dados combinados
                logging.info(f"Preenche os valores NaN nos dados combinados: {table}")
                for column in df_new.columns:
                    old_column = f'{column}_old'
                    if old_column in df_combined.columns:
                        df_combined[column] = df_combined[column].fillna(df_combined[old_column])
                        df_combined.drop(columns=[old_column], inplace=True)
                        
            else:
                # Cria um novo DataFrame
                logging.info(f"Arqui não existe no path informado: {blob}")
                logging.info(f"Criando um novo DataFrame: {table}")
                df_combined = df_new.astype(str)
                
                print(f"\n {df_combined.head()} \n")
                
            # Salva o DataFrame combinado (Converte para parquet)
            logging.info(f"Salva o DataFrame combinado (Converte para parquet): {table}")
            df_combined = df_combined.astype(str)
            
            # Para binários
            blob.upload_from_string(df_combined.to_parquet(index=False), content_type='application/octet-stream')
            
            # Padrão
            #blob.upload_from_string(df_combined.to_parquet(index=), content_type='application/octet-stream')
            
            logging.info(f'{table} {"=" * (80 - len(table))} {df_combined.shape}')   
            
            print("\n")                                 
                    
            #except psycopg2.Error as e:
            #    logging.info(f"Erro encontrado durante a conexão: {e}")
            #    print("\n\n")
            #    yield list_tables_names
        
        yield list_tables_names