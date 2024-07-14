import apache_beam as beam
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import secretmanager
import json
import logging
import sys
from pandas_gbq import to_gbq

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(levelname)s - %(message)s")

class LoadRaw(beam.DoFn):
    def process(self, element):        
        logging.info("Recuperando os dados da tabela controller")
        controler = pd.read_csv('controller/controler.csv')
        
        logging.info("Criando cliente para o Secret Manager")
        client = secretmanager.SecretManagerServiceClient()

        logging.info("Montando o nome do recurso do secret")
        name = "projects/872130982957/secrets/service-account-motors-word/versions/1"

        logging.info("Acessando o valor do secret")
        response = client.access_secret_version(name=name)

        logging.info("Decodificando o payload")
        secret_payload = response.payload.data.decode("UTF-8")

        logging.info("Convertendo payload JSON para dicionário")
        secret_dict = json.loads(secret_payload)

        logging.info("Criando credenciais a partir do dicionário do secret")
        credentials = service_account.Credentials.from_service_account_info(secret_dict)

        for table in element: 
            logging.info(f"Recuperando valores da tabela controller para a tabela {table}")
            target_bucket = str(controler.loc[controler['table'] == table, 'target_bucket'].iloc[0])
            target_folder_path = str(controler.loc[controler['table'] == table, 'target_folder_path'].iloc[0]) 
            project_id = str(controler.loc[controler['table'] == table, 'project_id'].iloc[0]) 
            bq_dataset_id = str(controler.loc[controler['table'] == table, 'bq_dataset_id'].iloc[0]) 
                
            project_id = project_id
            bq_dataset_id = bq_dataset_id
            
            logging.info(f"Configurando caminho do GCS para a tabela {table}")
            bucket_name = target_bucket
            path = f'{target_folder_path}/{table}.parquet'
            gcs_file_path = f'gs://{bucket_name}/{path}'
                   
            logging.info(f"Carregando dados do arquivo Parquet: {gcs_file_path}")
            try:
                df_parquet = pd.read_parquet(gcs_file_path)
            except Exception as e:
                logging.error(f"Erro ao carregar arquivo {gcs_file_path}: {e}")
                continue

            logging.info("Convertendo todas as colunas para string")
            df_parquet = df_parquet.astype(str)

            logging.info("Iniciando conexão com BigQuery")
            bq_client = bigquery.Client(credentials=credentials, project=project_id)

            logging.info("Referência da tabela no BigQuery")
            table_ref = f'{project_id}.{bq_dataset_id}.{table}'

            logging.info("Verificando se a tabela existe")
            try:
                bq_client.get_table(table_ref)
                table_exists = True
                logging.info("A tabela existe. Criando DataFrame...")           
                
            except Exception:
                table_exists = False

            if table_exists:
                logging.info("Tabela existe, truncando a tabela existente para salvar a nova versão")
                try:
                    query = f"SELECT * FROM `{table_ref}`"
                    df_bq = bq_client.query(query).to_dataframe()
                    df_bq = df_bq.astype(str)
                    logging.info("DataFrame criado com sucesso.")
                    
                    merge_column = df_parquet.columns[0]
                    logging.info(f"Recupera o nome da primeira coluna para alinhar o merge: {merge_column}")
                    
                    logging.info(f"Realiza o merge: {table}")
                    df_combined = pd.merge(df_bq, df_parquet, on=merge_column, how='outer', indicator=True, suffixes=('_old', ''))
                    
                    logging.info(f"Preenche os valores NaN nos dados combinados: {table}")
                    for column in df_parquet.columns:
                        old_column = f'{column}_old'
                        if old_column in df_combined.columns:
                            df_combined[column] = df_combined[column].fillna(df_combined[old_column])
                            df_combined.drop(columns=[old_column], inplace=True) 
                            
                    logging.info("Truncando a tabela existente para salvar a nova versão")
                    bq_client.query(f'TRUNCATE TABLE {table_ref}').result()
                        
                except Exception as e:
                    logging.error(f"Erro ao truncar a tabela {table_ref}: {e}")
                    continue
            
            else:
                df_combined = df_parquet
                
            logging.info(f"Carregando dados no BigQuery para a tabela {table}")
            try:
                to_gbq(df_combined, destination_table=f'{bq_dataset_id}.{table}', project_id=project_id, if_exists='replace', credentials=credentials)
                print("\n")
            except Exception as e:
                logging.error(f"Erro ao carregar dados no BigQuery para a tabela {table}: {e}")
                print("\n")
                continue
