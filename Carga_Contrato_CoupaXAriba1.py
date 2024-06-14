import requests
import json
import pandas as pd
import zipfile
import os

COUPA_URL_REQUEST = "https://gruponc-test.coupahost.com/oauth2/token"
Body_Request = {
    'client_id': '670dcc4fc8b7eeb1757492962b202eb0',
    'client_secret': 'fceadfdb129b8fcf1631bae3fbd4b9569ee5d097a861f3537204a7da2e995bcd',
    'scope': 'core.approval.read core.approval.write core.comment.read core.comment.write core.common.read core.common.write core.contract.read core.contract.write core.contracts_template.read core.easy_form_response.read core.easy_form_response.write core.easy_form.read core.easy_form.write core.item.read core.item.write core.purchase_order_change.read core.purchase_order_change.write core.purchase_order.read core.purchase_order.write core.requisition.assignment.read core.requisition.read core.requisition.write core.sourcing.read core.sourcing.response.read core.sourcing.response.write core.sourcing.write core.supplier.read core.user.read core.user.write email login offline_access openid profile',
    'grant_type': 'client_credentials'
}

df = pd.read_excel('Contratos.xlsx')
lista_id = df['ID'].tolist()
str_lista = ','.join(map(str, lista_id))

COUPA_API_URL_EXTRACAO = "https://gruponc-test.coupahost.com/api/contracts","?id[in]=",str_lista,'&fields=["id","number","name","type","description",{"custom_fields":{}},{"supplier":["number"]},"maximum_value",{"currency":["code"]},{"parent":["id","number"]},{"department":["id"]},"start-date","end-date","published-date","status",{"shipping_term":["code"]}]'
COUPA_API_URL_EXTRACAO = ''.join(COUPA_API_URL_EXTRACAO)

diretorio_csv = r'C:\Users\rpa.adm\Desktop\Arquivos'
os.makedirs(diretorio_csv, exist_ok=True)

response = requests.post(COUPA_URL_REQUEST, data=Body_Request)
if response.status_code == 200:
    access_token = json.loads(response.text).get("access_token")
    headers = {"Authorization": "Bearer " + access_token, "Accept": "application/json"}
    response_extracao = requests.get(COUPA_API_URL_EXTRACAO, headers=headers)

    if response_extracao.status_code == 200:
        text_respons_extracao = json.loads(response_extracao.text)
        df_contratos = pd.DataFrame(text_respons_extracao)

        df_contract_teams = pd.DataFrame(columns=['Workspace', 'ProjectGroup', 'Member'])

        contract_teams_list = []
        for index, contrato in df_contratos.iterrows():
            workspace = f'LCW{contrato["id"]}'
            project_group = 'Project Owner'
            member = contrato['custom-fields']['comprador-responsvel']['login']
            contract_teams_list.append({'Workspace': workspace, 'ProjectGroup': project_group, 'Member': member})
        
        df_contract_teams = pd.DataFrame(contract_teams_list)

        caminho_arquivo_csv = os.path.join(diretorio_csv, 'ContractTeams.csv')

        df_contract_teams.to_csv(caminho_arquivo_csv, index=False, encoding='utf-8-sig')

        def extrair_capa(contrato):
            campos_capa = ['id', 'name', 'number', 'status', 'type', 'start-date', 'end-date', 'maximum-value', 'published-date', 'currency']
            return {campo: contrato[campo] for campo in campos_capa if campo in contrato}

        def criar_excel_anexos(contrato):
            if 'mapa-comparativo' in contrato['custom-fields'] and contrato['custom-fields']['mapa-comparativo']:
                caminho_anexo = contrato['custom-fields']['mapa-comparativo'] + f"/{contrato['id']}"
                df_anexos = pd.DataFrame({'Contrato': [contrato['id']], 'Caminho': [caminho_anexo]})
                caminho_excel = os.path.join(diretorio_csv, f'Anexos_{contrato["id"]}.xlsx')
                df_anexos.to_excel(caminho_excel, index=False)

        def criar_csv_anexos(contrato):
            dados_anexos = {
                'ContractId': f'LCW{contrato["id"]}',
                'File': f'Anexos_{contrato["id"]}.xlsx',
                'Title': f'Anexos_{contrato["id"]}',
                'Folder': '',
                'Owner': '',
                'Status': ''
            }
            df_csv_anexos = pd.DataFrame([dados_anexos])
            caminho_csv_anexos = os.path.join(diretorio_csv, f'listagem_anexos_{contrato["id"]}.csv')
            df_csv_anexos.to_csv(caminho_csv_anexos, index=False, header=True)

        for contrato in df_contratos.to_dict('records'):
            criar_excel_anexos(contrato)
            criar_csv_anexos(contrato)
            capa = extrair_capa(contrato)
            caminho_arquivo_csv = os.path.join(diretorio_csv, f'capa_contrato_{contrato["id"]}.csv')
            pd.DataFrame([capa]).to_csv(caminho_arquivo_csv, index=False, encoding='utf-8-sig')

        def criar_arquivos_zip(diretorio, max_arquivos_por_zip=100):
            arquivos = [os.path.join(diretorio, arquivo) for arquivo in os.listdir(diretorio) if arquivo.endswith('.csv')]
            for i in range(0, len(arquivos), max_arquivos_por_zip):
                arquivos_zip = arquivos[i:i+max_arquivos_por_zip]
                zip_filename = os.path.join(diretorio, f'contratos_grupo_{i//max_arquivos_por_zip}.zip')
                with zipfile.ZipFile(zip_filename, 'w') as zipf:
                    for arquivo in arquivos_zip:
                        zipf.write(arquivo, os.path.basename(arquivo))
                        os.remove(arquivo)

        criar_arquivos_zip(diretorio_csv)
    else:
        print(f"Erro na extração: {response_extracao.status_code} - {response_extracao.text}")
else:
    print(f"Erro na solicitação: {response.status_code} - {response.text}")
