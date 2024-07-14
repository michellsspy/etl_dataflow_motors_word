from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import os
import pandas as pd
import pyarrow
from google.cloud import storage
import logging
import sys
from datetime import datetime
from google.cloud import secretmanager
import json
import tempfile

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(levelname)s - %(message)s")

# Recuperndo parametros de conexão para a variável de ambiente GOOGLE_APPLICATION_CREDENTIALS
# Criando o client
secret_client = secretmanager.SecretManagerServiceClient()

# Recuperando o nome do banco de dados
secret_name = "projects/872130982957/secrets/service-account-motors-word/versions/1"

# Acessando o valor da secret
response = secret_client.access_secret_version(name=secret_name)

# Decode o Payload
secret_payload = response.payload.data.decode("UTF-8")

# O payload é um Json - Convertendo para um dict
secret_dict = None
try:
    secret_dict = json.loads(secret_payload)
except json.JSONDecodeError as e:
    logging.error(f"Erro ao carregar JSON: {e}")
    logging.error(f"Conteúdo recebido: {secret_payload}")
    raise

with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as temp_file:
    json.dump(secret_dict, temp_file)
    os.chmod(temp_file.name, 0o600)
    
# Definir a variável de ambiente para o caminho do arquivo temporário    
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file.name

# Criando um marcador com data atual
formatted_datetime = datetime.now().strftime("%Y-%m-%d--%H-%M")

def main(argv=None):
    # Definindo os parametros
    options = PipelineOptions(
        flags=argv,
        project='motors-word-etl-process',
        runner='DataflowRunner',
        streaming=False,
        job_name=f'etl-dataflow-motors-word-{formatted_datetime}',
        temp_location='gs://bkt-motors-word/temp',
        staging_location='gs://bkt-motors-word/staging',
        #template_location=f'gs://bkt-motors-word/templates/etl-dataflow-motors-word-{formatted_datetime}',
        autoscaling_algorithm='THROUGHPUT_BASED',
        worker_machine_type='n1-standard-4',
        service_account_key_file='./keys',
        num_workers=1,
        max_num_workers=3,
        number_of_worker_harness_threads=2,
        disk_size_gb=50,
        region='southamerica-east1',
        zone='southamerica-east1-c',
        #worker_zone='southamerica-east1-a',
        project_id='motors-word-etl-process',
        staging_bucket='bkt-motors-word',
        save_main_session=False,
        #experiments='use_runner_v2',
        prebuild_sdk_container_engine='cloud_build',
        docker_registry_push_url='southamerica-east1-docker.pkg.dev/motors-word-etl-process/etl-dataflow-motors-word/motors-dev',
        sdk_container_image='southamerica-east1-docker.pkg.dev/motors-word-etl-process/etl-dataflow-motors-word/motors-dev:latest',
        sdk_location='container',
        requirements_file='./requirements.txt',
        metabase_file='./metadata.json',
        setup_file='./setup.py',
        service_account_email='sa-motors-word-etl-process@motors-word-etl-process.iam.gserviceaccount.com'
    )
    
    # Importando as funções dos pipelines
    from functions.load_landing import LoadLanding
    from functions.load_raw import LoadRaw
    
    # Criando o pipeline
    with beam.Pipeline(options=options) as pipeline:        
        load_landing = (
            pipeline
            | 'Start Pipeline' >> beam.Create([None])
            | 'Get Tables' >> beam.ParDo(LoadLanding())   
        )
        
        load_raw = (
            load_landing
            | 'Load Tables To BQ' >> beam.ParDo(LoadRaw())   
        )
        
if __name__ == '__main__':
    main()