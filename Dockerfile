#Pegando uma imagem mais leve do python
FROM python:3.10-slim
COPY --from=apache/beam_python3.10_sdk:2.55.0 /opt/apache/beam /opt/apache/beam
COPY --from=gcr.io/dataflow-templates-base/python310-template-launcher-base:20230622_RC00 /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

ARG WORKDIR=/template
RUN mkdir ${WORKDIR}

WORKDIR ${WORKDIR}

#Criando os argumento que serão passados no processo de build
ARG DB_USER 
ARG DB_PASSWORD
ARG DB_HOST
ARG DB_PORT
ARG DB_SERVICE

# Definindo as variáveis de ambiente com os argumentos
ENV DB_USER=${DB_USER}
ENV DB_PASSWORD=${DB_PASSWORD}
ENV DB_HOST=${DB_HOST}
ENV DB_PORT=${DB_PORT}
ENV DB_SERVICE=${DB_SERVICE}

# Definir variáveis de ambiente para arquivos Python principais
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/main.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py
ENV FLEX_TEMPLATES_TAIL_CMD_TIMEOUT_IN_SECS=30
ENV FLEX_TEMPLATES_NUM_LOG_LINES=1000

COPY . ${WORKDIR}

RUN pip install --upgrade pip
RUN pip install -U -r ${WORKDIR}/requirements.txt
RUN ls ${WORKDIR}

ENTRYPOINT ["/opt/apache/beam/boot"]
