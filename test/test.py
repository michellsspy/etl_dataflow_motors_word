import basedosdados as bd

# Para carregar o dado direto no pandas
df = bd.read_table(dataset_id='br_inep_sinopse_estatistica_educacao_basica',
table_id='etapa_ensino_serie',
billing_project_id="<YOUR_PROJECT_ID>")

print(df)