# motors_word_etl_process

---

## Projeto ETL para Extração, Transformação e Carga de Dados de Motors Word

Neste projeto, estamos desenvolvendo um processo ETL (Extração, Transformação e Carga) para otimizar a análise de dados provenientes da motors word. O objetivo é transformar dados brutos, armazenados em um banco de dados PostgreSQL, em insights acionáveis utilizando um conjunto robusto de ferramentas e tecnologias.

### Ferramentas e Tecnologias Utilizadas:

- **Dataflow e Apache Beam:** Para o processamento e transformação dos dados em escala.
- **Python:** Linguagem de programação principal para scripts e pipelines ETL.
- **Cloud Build e Artifact Registry:** Para automação de builds e armazenamento de artefatos.
- **BigQuery:** Para armazenamento e consulta de grandes volumes de dados transformados.
- **DBT (Data Build Tool):** Para gestão e versionamento de modelos de dados.
- **Secret Manager:** Para gerenciamento seguro de credenciais e informações sensíveis.
- **GitHub:** Para controle de versão e colaboração no desenvolvimento do projeto.

Através deste pipeline ETL, garantiremos que os dados da motors word sejam extraídos, transformados e carregados de maneira eficiente e segura, proporcionando uma base sólida para análises avançadas e tomadas de decisão estratégicas.

---

Quando se trata de dataflow, o Python 3.12 apresenta conflitos com a versão apache_beam[gcp]==2.55.0, que só aceita a instalação do apache_beam[gcp]==2.57.0. Considerando esse cenário em projetos, temos duas soluções possíveis:

Se eu desejar desenvolver com a versão beam 2.55, precisarei usar o Python 3.10.
Caso contrário, posso utilizar o Python 3.12, mas terei que usar a versão beam 2.57.
No meu caso, a versão beam 2.57 apresentou falhas durante a execução no ambiente do Dataflow.

---

### Instalando python 3.10

sudo apt update
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.10

### Instalando recursos para criar o venv

sudo apt-get install python3.10-venv python3.10-distutils

### Criando o venv

python3.10 -m venv venv

### Iniciando o ambiente venv

source venv/bin/activate

### Instalando as dependências
pip install apache_beam[gcp]==2.55.0

---
