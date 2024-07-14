import pandas as pd

controler = pd.read_csv('data/controler.csv')
table = 'cidades'

# Filtrando as linhas onde a coluna 'table' é igual a 'table' e selecionando a coluna 'comand_sql', 'target_bucket' e 'target_folder_path'
comand_sql = str(controler.loc[controler['table'] == table, 'comand_sql'].iloc[0])
target_bucket = str(controler.loc[controler['table'] == table, 'target_bucket'].iloc[0])
target_folder_path = str(controler.loc[controler['table'] == table, 'target_folder_path'].iloc[0])
table_name = str(controler.loc[controler['table'] == table, 'table'].iloc[0])

list_names = list(controler.loc[:, 'table'])
print(list_names)
