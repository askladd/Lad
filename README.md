#Author: Leonardo
#Rev.: José Vieira da Silva Filho

import os
import logging
import boto3
import pandas as pd
import pyodbc

from botocore.config  import Config
from datetime         import datetime
from datetime         import date
from datetime         import timedelta
from datetime         import datetime
from xml.dom          import minidom

class Get_ClassApoio:
    
    #LEITURA APP.CONFIG
    def AppConfig(self,pathAppConfig):

        #Lendo xml App.Config
        from xml.dom import minidom
        pathAppConfig=pathAppConfig+'\App.config'
        xmldoc = minidom.parse(pathAppConfig)
        logging.warning('(1), AppConfig()-Local de Arquivo App.Config : "{}"...'.format(pathAppConfig))
        itemlist = xmldoc.getElementsByTagName('add')

        #Parsing das Tags
        for s in itemlist:
            match s.attributes['key'].value:
                case "DBconnectionString_Dev":
                    cnnStringDev=s.attributes['value'].value 
                case "DBconnectionString_Prod":
                    cnnStringProd=s.attributes['value'].value 

        #Define se é Servidor ou Local
        if( os.environ['COMPUTERNAME']=='DAMOLANDIA' or os.environ['COMPUTERNAME']=='DIORAMA' or os.environ['COMPUTERNAME']=='CAMALAU'):
            AppConfig=cnnStringProd
        else:
            AppConfig=cnnStringDev

        logging.warning('(2), AppConfig()-Connection String : "{}"...'.format(AppConfig))

        return AppConfig 
    
    #CRIANDO DIRETORIOS INEXISTENTES
    def Criar_Path(self, pathNew):
        if not os.path.exists(pathNew):            
            os.makedirs(pathNew) 
            retorno = os.path.dirname
        else:
            retorno = pathNew #os.path.dirname      
        return retorno  

    #CONFIGURAR ARQUIVO DE LOG     
    def Configurar_Log(self,logfilename):
        log_format = '%(asctime)s:%(levelname)s:%(filename)s:%(message)s'
        #logger = logging.getLogger('root')
        logging.basicConfig ( 
                    filename=logfilename,
                    filemode='a',
                    level=logging.INFO,
                    format=log_format )
        #logger = logging.getLogger('root')
        return logfilename
    
    #CONECTA AWS API CLIENT
    def Connect_AWS_Client(self,filepath_chaves_aws,retorno, account, ambiente, verify):
        
        if(retorno=='metricas'):
            servico='cloudwatch'
        elif(retorno=='instancias'):
            servico='ec2'
        
        dados = pd.read_json(filepath_chaves_aws)        
        param = '{}_{}'        
        region_name = dados['region_name'][0]        
        dados = dados[param.format(account, ambiente)]        
        key_id     = dados[0]        
        access_key = dados[1]
        
        #Definição da região em que fica o ambiente
        my_config = Config(region_name = region_name)

        #Definição do client de conexão as APIS do AWS
        client = boto3.client(servico,
                            config=my_config,
                            aws_access_key_id='{}'.format(key_id),
                            aws_secret_access_key='{}'.format(access_key),
                            verify=verify)
        
        #logging.warning('(1), Connect_AWS_Client()-servico : "{}"...'.format(servico))
        #logging.warning('(2), Connect_AWS_Client()-config : "{}"...'.format(my_config))
        

        return client
    
    def Tempo_execucao(self,inicio='', fim=''):
        if(inicio==''):
            inicio = datetime.now()            
            return inicio
        else:
            fim    = datetime.now()
            tempo_exec = (fim-inicio).total_seconds()
        
        return tempo_exec

class Get_DbConnect:

    #Função para conectar À iNSTANCIA sql
    def connectDB(self):
        if( os.environ['COMPUTERNAME']=='DAMOLANDIA' 
                                or os.environ['COMPUTERNAME']=='DIORAMA' 
                                or os.environ['COMPUTERNAME']=='CAMALAU'):    
            pathAppConfig='D:/Metricas/Scripts/SQLServer/Release'       
        else:
            pathAppConfig='C:/Metricas/Scripts/SQLServer/Release'
        print (pathAppConfig)
        logging.warning('(4), connectDB(),Dir AppConfig: "{}"...'.format(pathAppConfig))

        oGet_Apoio = Get_ClassApoio()
        cnnString=oGet_Apoio.AppConfig(pathAppConfig)    

        #Cria o conector que possibilita a interação com o banco de dados SQL Server
        cnxn = pyodbc.connect(cnnString)
        cursor = cnxn.cursor()
        #logging.warning('(4), connectDB(),String de Conexão: "{}"...'.format(cnnString))
        return cnxn

    #Função que realiza a consulta dos dados SQL
    def consulta_dados(self,tabela, consulta='SELECT * FROM ', filtro=''):
        consulta = consulta+tabela+filtro

        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        
        dados = pd.read_sql_query(consulta, cnxn)
        cnxn.close()
        return dados 
    
    #Função que insere dados SQL
    def inserir_dados(self,dados, tabela):
        
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()

        cursor = cnxn.cursor()
        
        colunas = str(dados.columns.values).strip('[]').replace("'", '').replace(' ', ',').replace('\n', '')
        
        for index, linha in dados.iterrows():
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            
            consulta = """
                        INSERT INTO {}({}) 
                        VALUES({})
                    """.format(tabela, colunas, valores.replace("'NaT'", 'null').replace("'nan'", 'null'))
            
            cursor.execute(consulta)
            cnxn.commit()

        cursor.close()
        cnxn.close()
        print('{} registros inseridos na tabela {}!!!'.format(len(dados), tabela))
        logging.warning('(1), atualizar_dados(): "{}"...'.format('{} registros inseridos na tabela {}!!!'.format(len(dados), tabela)))

    def inserir_incidentes(dados):        
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        for index, linha in dados.iterrows():
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            
            consulta = """
                        INSERT INTO tb_relatorio_cptm_incidentes(Incidente, ID, Velocidade, Redundancia, Servico, Valor, Area, Dt_solicitacao, 
                                    Parada_relogio, Dt_encerramento, Causa_cliente, Interrupcao, Tempo_paralisacao, Encerramento, Orgao, Mes_referencia, Glosa, T_ind_hora) 
                        VALUES("""+valores+""")
                    """

            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de recursos no SQL
    def inserir_recursos(dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        for index, linha in dados.iterrows():
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            consulta = """
                        INSERT INTO tb_relatorio_cptm_recursos(ID, Orgao, Area_rural, Redundancia, Servico, Capacidade, Mes_referencia, valor) 
                        VALUES("""+valores+""")
                    """

            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de solicitacoes no SQL
    def inserir_solicitacoes(dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        cnxn.close()
        
        for index, linha in dados.iterrows():
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]').replace('NaT', '')

            consulta = """
                        INSERT INTO tb_relatorio_cptm_solicitacoes(ID, Protocolo, Tipo_solicitacao, Area_rural, Redundancia, Servico, 
                                        Capacidade, Dt_entrada, Dt_faturamento, Status, Tempo_execucao, data_atualizacao,
                                        indicador, meta, status_sla, Mes_referencia, Valor, Penalidade) 
                        VALUES("""+valores+""")
                    """

            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de valores no SQL
    def inserir_valores(dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        last_id = oGet_DbConnect.consulta_dados('tb_valores_por_capacidade_cptm_intragov')
        last_id = last_id.ID.max()
        if(pd.isna(last_id)):
            last_id=0
            
        for index, linha in dados.iterrows():
            last_id+= 1
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            consulta = """
                        INSERT INTO tb_valores_por_capacidade_cptm_intragov(id,Velocidade, Valor_SCM, Valor_SAI, Valor_STI, Redundancia, Inicio_vigencia, Ano_vigencia) 
                        VALUES("""+str(last_id)+""","""+valores+""")
                    """

            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de documentos no SQL
    def inserir_documentos(dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
            
        for index, linha in dados.iterrows():
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            
            consulta = """
                        INSERT INTO tb_controle_documentos(identificador, relatorio, cliente, mes_referencia) 
                        VALUES("""+valores+""")
                    """
            
            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de controle de etls no SQL
    def inserir_controles(dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        last_id = oGet_DbConnect.consulta_dados('tb_controle_etls_cptm')
        last_id = last_id.id.max()
        if(pd.isna(last_id)):
            last_id=0
            
        for index, linha in dados.iterrows():
            last_id+= 1
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            consulta = """
                        INSERT INTO tb_controle_etls_cptm(id,arquivo, import, transform, etl) 
                        VALUES('"""+str(last_id)+"""',"""+valores+""")
                    """
            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de mes referencia no SQL
    def insert_mes_ref(dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()

        lst_mes_name = {'01':'Janeiro', '02':'Fevereiro', '03':'Março'   , '04':'Abril'  , '05':'Maio'    , '06':'Junho',
                        '07':'Julho'  , '08':'Agosto'   , '09':'Setembro', '10':'Outubro', '11':'Novembro', '12':'Dezembro'}

        for dia in dados:
            mes_ref      = str(format(dia,'%m/%Y'))
            periodo_mes  = str(format(dia, '%b-%Y')).upper()
            ano          = str(format(dia, '%Y'))
            mes          = str(format(dia, '%m'))
            nome_mes     = str(lst_mes_name[format(dia, '%m')])
            consulta     = """
                        INSERT INTO tb_mes_referencia(mes_ref, periodo_mes, ano_num, mes_num, mes_name) 
                        VALUES('"""+mes_ref+"""','"""+periodo_mes+"""',"""+ano+""","""+mes+""",'"""+nome_mes+"""')
                    """
            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de consistencias no SQL
    def input_consistencias(dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()

        for index, linha in dados.iterrows():
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            consulta = """
                        INSERT INTO tb_consistencias_dados_CPTM(Mes_referencia,Incidente,ID,Velocidade,Velocidade_solicitacoes,
                                            Velocidade_recursos,Redundancia,Redundancia_solicitacoes,Redundancia_recursos
                                            ) 
                        VALUES("""+valores+""")
                    """
            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de AWS no SQL
    def input_AWS(tabela, dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()

        for index, linha in dados.iterrows():
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            consulta = """
                        INSERT INTO tb_AWS_{}(Data,Status,Metrica,Resource,Instancia,ID_metrica,ambiente,
                                            Data_criacao,Data_encerramento,StopReason,State,KeyName,Region
                                            ) 
                        VALUES({})
                    """.format(tabela, valores)
            
            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de AWS desenvolve sp no SQL
    def input_AWS_desenvolvesp(tabela, dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()

        for index, linha in dados.iterrows():
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            consulta = """
                        INSERT INTO tb_AWS_{}_desenvolvesp(Data,Status,Metrica,Resource,Instancia,ID_metrica,ambiente,
                                                        Data_criacao,Data_encerramento,StopReason,State,KeyName,Region
                                            ) 
                        VALUES({})
                    """.format(tabela, valores)
            
            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de AWS indisponibilidade no SQL
    def inserir_indisp_aws(tabela, dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        last_id = oGet_DbConnect.consulta_dados('tb_controle_etls_cptm')
        last_id = last_id.id.max()
        if(pd.isna(last_id)):
            last_id=0
            
        for index, linha in dados.iterrows():
            last_id+= 1
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            consulta = """
                        INSERT INTO tb_AWS_{}_indisponibilidade(Data,Status,Metrica,Resource,Instancia,ID_metrica,ambiente,
                                                                Data_criacao,Data_encerramento,StopReason,State,KeyName,Region) 
                        VALUES({})
                        """.format(tabela, valores)
            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de AWS indisponibilidade desenvolve no SQL
    def inserir_indisp_aws_desenvolvesp(tabela, dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        last_id = oGet_DbConnect.consulta_dados('tb_controle_etls_cptm')
        last_id = last_id.id.max()
        if(pd.isna(last_id)):
            last_id=0
            
        for index, linha in dados.iterrows():
            last_id+= 1
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            consulta = """
                        INSERT INTO tb_AWS_{}_desenvolvesp_indisponibilidade(Data,Status,Metrica,Resource,Instancia,ID_metrica,ambiente,
                                                                Data_criacao,Data_encerramento,StopReason,State,KeyName,Region) 
                        VALUES({})
                        """.format(tabela, valores)
            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de instancias no SQL
    def inserir_instancias(dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        for index, linha in dados.iterrows():
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            consulta = """
                        INSERT INTO tb_instancias_aws_por_mes(InstanceId,Grupo,Status,Data_inicio,
                                                            Data_encerramento,Razao_desativacao,Ambiente) 
                        VALUES({})
                        """.format(valores)
            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de status das instancias no SQL
    def inserir_status_instancias(dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        for index, linha in dados.iterrows():
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            consulta = """
                        INSERT INTO tb_aws_check_status_instancias_telemedicina(InstanceId,LaunchTime,State,
                                                                                StateTransitionReason,KeyName,ambiente) 
                        VALUES({})
                        """.format(valores)
            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que inseri dados de status das instancias desenvolve sp no SQL
    def inserir_status_instancias_desenvolvesp(dados):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        for index, linha in dados.iterrows():
            valores = [f"{x}" for x in linha]
            valores = str(valores)
            valores = valores.strip('[]')
            consulta = """
                        INSERT INTO tb_aws_check_status_instancias_desenvolve_sp(InstanceId,LaunchTime,State,
                                                                                StateTransitionReason,KeyName,ambiente) 
                        VALUES({})
                        """.format(valores)
            cursor.execute(consulta)
            cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que atualiza os dados de controle de etls cptm no SQL
    def atualizar_controle(estagio, valor, arquivo='', filtro=''):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        if(filtro==''):
            filtro = " WHERE [arquivo]='{}'".format(arquivo)
        
        consulta = """UPDATE tb_controle_etls_cptm
                    SET ["""+estagio+"""]="""+valor+filtro
        
        cursor.execute(consulta)
        cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que atualiza os dados de instancias no SQL
    def update_dados_instancia(instancia, launchtime, state, statetransition, keyname, arquivo='', filtro=''):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        if(filtro==''):
            filtro = " WHERE [InstanceId]='{}'".format(instancia)
        
        consulta = """UPDATE tb_aws_check_status_instancias_telemedicina
                    SET [LaunchTime]='{}',
                        [State]='{}', 
                        [StateTransitionReason]='{}', 
                        [KeyName]='{}'""".format(launchtime,state,statetransition,keyname)+filtro
        
        cursor.execute(consulta)
        cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que atualiza os dados de instancias desenvolve sp no SQL
    def update_dados_instancia_desenvolvesp(instancia, launchtime, state, statetransition, keyname, arquivo='', filtro=''):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        if(filtro==''):
            filtro = " WHERE [InstanceId]='{}'".format(instancia)
        
        consulta = """UPDATE tb_aws_check_status_instancias_desenvolve_sp
                    SET [LaunchTime]='{}',
                        [State]='{}', 
                        [StateTransitionReason]='{}', 
                        [KeyName]='{}'""".format(launchtime,state,statetransition,keyname)+filtro
        
        cursor.execute(consulta)
        cnxn.commit()
        cursor.close()
        cnxn.close()

    #Função que atualiza os dados de instancias desenvolve sp no SQL
    def atualizar_dados(tabela, valores, filtro=''):
        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        consulta = "UPDATE {} {} {}".format(tabela,valores,filtro)
        
        cursor.execute(consulta)
        cnxn.commit()
        cursor.close()
        cnxn.close()
        
        print('Registro atualizado com sucesso na tabela {}!!!'.format(tabela))
        logging.warning('(1), atualizar_dados(): "{}"...'.format('Registro atualizado com sucesso na tabela {}!!!'.format(tabela)))

    #Função que deleta os dados no SQL
    def deletar_dados(self,tabela, filtro=''):

        oGet_DbConnect = Get_DbConnect()
        cnxn=oGet_DbConnect.connectDB()
        cursor = cnxn.cursor()
        
        dados = oGet_DbConnect.consulta_dados(tabela, filtro=filtro)
        
        consulta = """
                    DELETE FROM """+tabela+filtro+"""
                """

        cursor.execute(consulta)
        
        cnxn.commit()
        cursor.close() 
        cnxn.close()
        print('{} registros deletados da tabela {}!!!'.format(len(dados), tabela))

class Get_CheckDados:

    def check_status_instancias_ec2(self,pathAWS_ISTcsv,backup,verify,filepath_chaves_aws):

        oGet_ClassApoio = Get_ClassApoio()
        oGet_DbConnect = Get_DbConnect()       

        #Registra o tempo inicial da execução do script
        inicio = oGet_ClassApoio.Tempo_execucao()
        logging.warning('(1), check_status_instancias_ec2()-Tempo inicial da execução do script : "{}"...'.format(inicio))
      
        #Conecta aos clients da AWS de Telemedicina nos ambientes de produção e homologação
        #Client de produção
        client_prod = oGet_ClassApoio.Connect_AWS_Client(filepath_chaves_aws,'instancias', 'telemedicina', 'producao'   , verify)

        #Client de homologação
        client_homo = oGet_ClassApoio.Connect_AWS_Client(filepath_chaves_aws,'instancias', 'telemedicina', 'homologacao', verify)

        #Declara o DataFrame para as instancias que será utilizado no sript
        df_instancias = pd.DataFrame()

        #Faz a consulta aos dados no client da AWS e normaliza os dados
        instancias = pd.json_normalize(client_prod.describe_instances()['Reservations'])
        instancias['ambiente'] = 'producao'
        instancias = instancias.append(pd.json_normalize(client_homo.describe_instances()['Reservations']))
        instancias.loc[instancias['ambiente'].isna(), 'ambiente'] = 'homologacao'
        instancias = instancias.reset_index(drop=True)

        #Executa as transformações nos dados das instâncias
        a = 0
        for index, instancia in instancias.iterrows():
            df_instancias = df_instancias.append(pd.json_normalize(instancia['Instances'])[['InstanceId', 'LaunchTime', 'State.Name', 'StateTransitionReason', 'KeyName']])
            df_instancias = df_instancias.reset_index(drop=True)            
            df_instancias.loc[a, 'ambiente'] = instancia['ambiente']
            a+=1

        #Converte os dados da coluna LaunchTime no formato de data/hora especificado
        df_instancias['LaunchTime'] = df_instancias['LaunchTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        instancias = oGet_DbConnect.consulta_dados('tb_aws_check_status_instancias_telemedicina')
        try:
            instancias['LaunchTime'] = instancias['LaunchTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            y=0

        #Compara as informações entre as instâncias para efetuas or tratamentos caso necessário
        df_instancias_1 = df_instancias
        for index, linha in df_instancias.iterrows():
            df = instancias.loc[instancias['InstanceId']==linha['InstanceId']]
            if(not df.empty):
                if((df['LaunchTime'].values[0] ==linha['LaunchTime']) &
                        (df['State'].values[0] ==linha['State.Name']) &
                        (df['StateTransitionReason'].values[0] ==linha['StateTransitionReason']) &
                        (df['KeyName'].values[0] ==linha['KeyName'])):
                    df_instancias_1 = df_instancias_1.drop(index)
                else:
                    valores = ("SET [LaunchTime]='{}', [State]='{}', [StateTransitionReason]='{}'," +
                                "[KeyName]='{}'").format(linha['LaunchTime'], linha['State.Name'], 
                                                        linha['StateTransitionReason'], linha['KeyName'])
                    
                    filtro = " WHERE [InstanceId]='{}'".format(linha['InstanceId'])                    
                    oGet_DbConnect.atualizar_dados('tb_aws_check_status_instancias_telemedicina', valores, filtro)                    
                    df_instancias_1 = df_instancias_1.drop(index)

        df_instancias_1.columns = ['InstanceId', 'LaunchTime', 'State', 
                                'StateTransitionReason', 'KeyName', 'ambiente']

        #Insere os dados das instâncias no SQL Server
        oGet_DbConnect.inserir_dados(df_instancias_1, 'tb_aws_check_status_instancias_telemedicina')

        #Lista as métricas as quais serão consultadas
        lst_metricas = ['StatusCheckFailed', 'StatusCheckFailed_Instance', 'StatusCheckFailed_System']

        #Verifica a existênciadas instâncias para atualização dos status
        instancias = instancias[instancias['StateTransitionReason']==""]
        for index, linha in instancias.iterrows():
            df = df_instancias.loc[(df_instancias['InstanceId']==linha['InstanceId']) & (linha['StateTransitionReason']=="")]
            if(df.empty):
                    valores = ("SET [LaunchTime]='{}', [State]='{}', [StateTransitionReason]='{}'," +
                                "[KeyName]='{}'").format(linha['LaunchTime'], 'stopped', 
                                                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), linha['KeyName'])
                    
                    filtro = " WHERE [InstanceId]='{}'".format(linha['InstanceId'])                    
                    oGet_DbConnect.atualizar_dados('tb_aws_check_status_instancias_telemedicina',valores, filtro)

        #Consulta os dados das instâncias no SQL Server
        instancias = oGet_DbConnect.consulta_dados('tb_aws_check_status_instancias_telemedicina')

        #Execut as verificações das instãncias por métrica
        for metrica in lst_metricas:
            for index, linha in instancias.iterrows():
                inicio=linha['LaunchTime']
                fim   =linha['StateTransitionReason'].replace(' GMT)', '').replace('User initiated (', '').replace('User initiated', '')
                if(linha['State']!='running'):
                    oGet_DbConnect.deletar_dados("tb_AWS_{}_indisponibilidade".format(metrica)
                            , filtro=" WHERE (Data<'{}' or Data>'{}') and Instancia = '{}'".format(inicio, fim, linha['InstanceId']))
                else:
                    oGet_DbConnect.deletar_dados("tb_AWS_{}_indisponibilidade".format(metrica)
                            , filtro=" WHERE Data<'{}' and Instancia = '{}'".format(inicio, linha['InstanceId']))

        #Executa o registro do backup caso a execução seja feita ná VM
        if(backup==True):
            
            #Define o caminho para o backup da coleta
            caminho_csv = pathAWS_ISTcsv
                
            #Consulta os dados e registra o backup da coleta
            instancias = oGet_DbConnect.consulta_dados('tb_aws_check_status_instancias_telemedicina')
            instancias.to_csv(caminho_csv, index=False, sep=";", quoting=1)

        #Registra o tempo final e o tempo total da execução do script
        tempo_exec = oGet_ClassApoio.Tempo_execucao(inicio)
        tempo_exec

        return tempo_exec

    def check_status_instancias_Desenvolvesp_EC2(self,pathAWS_ISDcsv,backup,verify,filepath_chaves_aws):

        oGet_ClassApoio = Get_ClassApoio()
        oGet_DbConnect = Get_DbConnect()        

        #Registra o tempo inicial da execução do script
        inicio = oGet_ClassApoio.Tempo_execucao()
        logging.warning('(1), check_status_instancias_ec2()-Tempo inicial da execução do script : "{}"...'.format(inicio))
      
        #Conecta aos clients da AWS de Telemedicina nos ambientes de produção e homologação
        #Client de produção
        client_prod = oGet_ClassApoio.Connect_AWS_Client(filepath_chaves_aws,'instancias', 'desenvolvesp', 'producao'   , verify)

        
        #Declara o DataFrame para as instancias que será utilizado no sript
        df_instancias = pd.DataFrame()

        #Faz a consulta aos dados no client da AWS e normaliza os dados
        instancias = pd.json_normalize(client_prod.describe_instances()['Reservations'])
        instancias['ambiente'] = 'producao'
        instancias = instancias.reset_index(drop=True)  

        #Executa as transformações nos dados das instâncias
        a = 0
        for index, instancia in instancias.iterrows():
            df_instancias = df_instancias.append(pd.json_normalize(instancia['Instances'])[['InstanceId', 'LaunchTime', 'State.Name', 'StateTransitionReason', 'KeyName']])
            df_instancias = df_instancias.reset_index(drop=True)            
            df_instancias.loc[a, 'ambiente'] = instancia['ambiente']
            a+=1

        #Converte os dados da coluna LaunchTime no formato de data/hora especificado
        df_instancias['LaunchTime'] = df_instancias['LaunchTime'].dt.strftime('%Y-%m-%d %H:%M:%S')    
        instancias = oGet_DbConnect.consulta_dados('tb_aws_check_status_instancias_desenvolve_sp')
        try:
            instancias['LaunchTime'] = instancias['LaunchTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            y=0

        #Compara as informações entre as instâncias para efetuas or tratamentos caso necessário
        df_instancias_1 = df_instancias
        for index, linha in df_instancias.iterrows():
            df = instancias.loc[instancias['InstanceId']==linha['InstanceId']]
            if(not df.empty):
                if((df['LaunchTime'].values[0]               ==linha['LaunchTime']) &
                        (df['State'].values[0]                    ==linha['State.Name']) &
                        (df['StateTransitionReason'].values[0]    ==linha['StateTransitionReason']) &
                        (df['KeyName'].values[0]                  ==linha['KeyName'])):
                    df_instancias_1 = df_instancias_1.drop(index)
                else:
                    valores = ("SET [LaunchTime]='{}', [State]='{}', [StateTransitionReason]='{}'," +
                                "[KeyName]='{}'").format(linha['LaunchTime'], linha['State.Name'], 
                                                        linha['StateTransitionReason'], linha['KeyName'])                    
                    filtro = " WHERE [InstanceId]='{}'".format(linha['InstanceId'])                    
                    oGet_DbConnect.atualizar_dados('tb_aws_check_status_instancias_desenvolve_sp',valores, filtro)                    
                    df_instancias_1 = df_instancias_1.drop(index)
        df_instancias_1.columns = ['InstanceId', 'LaunchTime', 'State', 
                           'StateTransitionReason', 'KeyName', 'ambiente']

        #Insere os dados das instâncias no SQL Server
        oGet_DbConnect.inserir_dados(df_instancias_1, 'tb_aws_check_status_instancias_desenvolve_sp')

        #Verifica a existência das istâncias para atualização dos status
        instancias = instancias[instancias['StateTransitionReason']==""]
        for index, linha in instancias.iterrows():
            df = df_instancias.loc[df_instancias['InstanceId']==linha['InstanceId']]
            if(df.empty):
                    valores = ("SET [LaunchTime]='{}', [State]='{}', [StateTransitionReason]='{}'," +
                                "[KeyName]='{}'").format(linha['LaunchTime'], 'stopped', 
                                                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), linha['KeyName'])                    
                    filtro = " WHERE [InstanceId]='{}'".format(linha['InstanceId'])                    
                    oGet_DbConnect.atualizar_dados('tb_aws_check_status_instancias_desenvolve_sp',valores, filtro)

        #Executa o registro do backup caso a execução seja feita ná VM
        if(backup==True):

            #Define o caminho para o backup da coleta
            caminho_csv = pathAWS_ISDcsv

            #Coleta as informações e salva o arquivo de backup
            instancias = oGet_DbConnect.consulta_dados('tb_aws_check_status_instancias_desenvolve_sp')
            instancias.to_csv(caminho_csv, index=False, sep=";", quoting=1)

        #Registra o tempo final e o tempo total da execução do script
        tempo_exec = oGet_ClassApoio.Tempo_execucao(inicio)
        tempo_exec     

        return tempo_exec
    
    def Conexao_CloudWatch_prod(self,pathAWS_TELcsv,backup,verify,filepath_chaves_aws):

        oGet_ClassApoio = Get_ClassApoio()
        oGet_DbConnect = Get_DbConnect()
        

        #Conecta aos clients da AWS de Telemedicina nos ambientes de produção
        #Client de produção
        client_prod = oGet_ClassApoio.Connect_AWS_Client(filepath_chaves_aws,'metricas', 'telemedicina', 'producao'   , verify)

        #Consulta dos dados das instâncias no SQL Server
        instancias = oGet_DbConnect.consulta_dados('tb_aws_check_status_instancias_telemedicina', filtro=" WHERE ambiente='producao'")
        
        #Define o intervalo de execução das coletas
        datas = datetime.today()-timedelta(days=1)

        #Definições iniciais das instâncias para a execução dos scripts
        df_instancias = instancias[(instancias['StateTransitionReason']=='') | 
                (instancias['StateTransitionReason'].str.replace('User initiated \(', '').str.replace(' GMT\)', '').str.replace('User initiated', '')>=datas.strftime('%Y-%m-%d 00:00:00'))]

        #Executa alguns tratamentos que possibilitam as execuções das coletas
        df_instancias['LaunchTime'] = df_instancias['LaunchTime']+timedelta(hours=-3)

        df_instancias['Data_encerramento'] = datetime.today()-timedelta(days=15)
        df_instancias['Data_encerramento'] = df_instancias['Data_encerramento'].astype('datetime64')

        df_instancias['Data_encerramento']   = df_instancias['Data_encerramento'].fillna(datetime.today().strftime('%Y-%m-%d 00:00:00'))

        #Define as metricas que serão consultadas nas instâncias
        lst_metricas = ['StatusCheckFailed', 'StatusCheckFailed_Instance', 'StatusCheckFailed_System']

        #Cria a lista com as datas para a execução das coletas
        lst_data = pd.date_range(date.today()-timedelta(days=1), date.today()-timedelta(days=1))

        #Define o caminho para a execução dos backups das coletas
        caminho_csv = pathAWS_TELcsv + '{}/telemedicina_{}_{}_producao.csv'

        #Executa as coletas e transformações dos checks nas instâncias
        for dia in lst_data:

            print('Data de Coleta: {}!!!'.format(dia))
            logging.warning('(1),Conexao_CloudWatch_prod(1): "{}"...'.format('Data de Coleta: {}!!!'.format(dia)))

            st_date = format(dia, '%Y-%m-%d 00:00:00')
            ed_date = format(dia+timedelta(days=1), '%Y-%m-%d 00:00:00')
            
            st_date = datetime.strptime(st_date, '%Y-%m-%d %H:%M:%S')
            ed_date = datetime.strptime(ed_date, '%Y-%m-%d %H:%M:%S')
            
            for metrica in lst_metricas:
                dados_check = pd.DataFrame()
                dados_indis = pd.DataFrame()
                
                for index, instancia in df_instancias.iterrows():
            
                    response_intarator = client_prod.get_metric_data(
                        MetricDataQueries=[
                            {
                                'Id': 'e1',
                                'MetricStat': {
                                    'Metric': {
                                        'MetricName': '{}'.format(metrica),
                                        'Namespace': 'AWS/EC2',
                                        'Dimensions': [
                                            {
                                                'Name': 'InstanceId',
                                                'Value': '{}'.format(instancia['InstanceId'])
                                            },
                                        ]
                                    },
                                    'Period': 60,
                                    'Stat': 'Maximum'
                                }
                            }
                        ],
                        StartTime=st_date,
                        EndTime=ed_date)

                    Datas = pd.json_normalize(pd.json_normalize(pd.json_normalize(response_intarator)['MetricDataResults'])[0])['Timestamps']
                    Values = pd.json_normalize(pd.json_normalize(pd.json_normalize(response_intarator)['MetricDataResults'])[0])['Values'][0]
                    a=0
                    name = metrica
                    name = pd.DataFrame()

                    for Data in Datas[0]:
                        name.loc[a, ['Data', 'Status']] = Data, Values[a]
                        a+=1

                    dados_filtro = pd.DataFrame()

                    if len(name)>0:
                        name.Data                 = name.Data.astype('datetime64')
                        name['Metrica']           = metrica
                        name['Resource']          = 'AWS/EC2'
                        name['Instancia']         = instancia['InstanceId']
                        name['ID_metrica']        = 'InstanceId'
                        name['ambiente']          = 'producao'
                        name['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        name['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        name['StopReason']        = instancia['StateTransitionReason']
                        name['State']             = instancia['State']
                        name['KeyName']           = instancia['KeyName']
                        name['region']            = 'sa-east-1'
                        
        
                        min_time = st_date
                        max_time = ed_date

                        date_list             = pd.DataFrame()
                        date_list['Data']     = pd.date_range(min_time.date(), max_time.date(), freq= 'min')[:-1]
                        date_list['Status_1'] = 1

                        dados_filtro = name.set_index('Data').join(date_list.set_index('Data'), how='right').reset_index()

                        dados_filtro = dados_filtro[dados_filtro['Status'].isna()]

                        dados_filtro['Status']            = 2
                        dados_filtro['Resource']          = 'AWS/EC2'
                        dados_filtro['Instancia']         = instancia['InstanceId']
                        dados_filtro['Metrica']           = metrica
                        dados_filtro['ID_metrica']        = 'InstanceId'
                        dados_filtro['ambiente']          = 'producao'
                        dados_filtro['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['StopReason']        = instancia['StateTransitionReason']
                        dados_filtro['State']             = instancia['State']
                        dados_filtro['KeyName']           = instancia['KeyName']
                        dados_filtro['region']            = 'sa-east-1'
                        
                        dados_filtro = dados_filtro[['Data','Status','Metrica','Resource','Instancia',
                                                    'ID_metrica','ambiente','Data_criacao','Data_encerramento',
                                                    'StopReason','State', 'KeyName', 'region']]
                        
                        
                        
                    elif(len(name)==0 and dia.date()>=instancia['LaunchTime'].date()):
                        min_time = st_date
                        max_time = ed_date

                        date_list             = pd.DataFrame()
                        date_list['Data']     = pd.date_range(min_time.date(), max_time.date(), freq= 'min')[:-1]
                        date_list['Status_1'] = 1     

                        dados_filtro = date_list

                        dados_filtro['Status']            = 2
                        dados_filtro['Resource']          = 'AWS/EC2'
                        dados_filtro['Instancia']         = instancia['InstanceId']
                        dados_filtro['Metrica']           = metrica
                        dados_filtro['ID_metrica']        = 'InstanceId'
                        dados_filtro['ambiente']          = 'producao'
                        dados_filtro['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['StopReason']        = instancia['StateTransitionReason']
                        dados_filtro['State']             = instancia['State']
                        dados_filtro['KeyName']           = instancia['KeyName']
                        dados_filtro['region']            = 'sa-east-1'
                        
                        dados_filtro = dados_filtro[['Data','Status','Metrica','Resource','Instancia',
                                                    'ID_metrica','ambiente','Data_criacao','Data_encerramento',
                                                    'StopReason','State', 'KeyName', 'region']]

                        
                    dados_check = dados_check.append(name)
                    dados_indis = dados_indis.append(dados_filtro)
                    
                #Executa o backup caso a execução seja realizada na VM
                if(backup==True):
                    pathcsvbk=(pathAWS_TELcsv+'{}/').format(metrica.lower())
                    pathcsvbk = oGet_ClassApoio.Criar_Path(str(pathcsvbk)) 
                    dados_check.to_csv(caminho_csv.format(metrica.lower(), metrica.lower(), Data.strftime('%d%m%Y'))
                                                            , index=False, sep=";", quoting=1)
                oGet_DbConnect.inserir_dados(dados_check, 'tb_AWS_{}'.format(metrica))

                #Executa o backup caso a execução seja realizada na VM
                if(backup==True):
                    pathcsvbk=(pathAWS_TELcsv+'{}/').format(metrica.lower())
                    dados_indis.to_csv(caminho_csv.format(metrica.lower(), metrica.lower(), Data.strftime('indisponibilidade_%d%m%Y'))
                                        , index=False, sep=";", quoting=1)

                oGet_DbConnect.inserir_dados(dados_indis, 'tb_AWS_{}_indisponibilidade'.format(metrica))

        return dia

    def Conexao_CloudWatch_homol(self,pathAWS_TELcsv,backup,verify,filepath_chaves_aws):

        oGet_ClassApoio = Get_ClassApoio()
        oGet_DbConnect = Get_DbConnect()       

        #Conecta aos clients da AWS de Telemedicina nos ambientes de produção
        #Client de produção
        client_homol = oGet_ClassApoio.Connect_AWS_Client(filepath_chaves_aws,'metricas', 'telemedicina', 'homologacao'  , verify)

        #Consulta dos dados das instâncias no SQL Server
        instancias = oGet_DbConnect.consulta_dados('tb_aws_check_status_instancias_telemedicina', filtro=" WHERE ambiente='homologacao'")
        
        #Define o intervalo de execução das coletas
        datas = datetime.today()-timedelta(days=1)

        #Definições iniciais das instâncias para a execução dos scripts
        df_instancias = instancias[(instancias['StateTransitionReason']=='') | 
                (instancias['StateTransitionReason'].str.replace('User initiated \(', '').str.replace(' GMT\)', '').str.replace('User initiated', '')>=datas.strftime('%Y-%m-%d 00:00:00'))]

        #Executa alguns tratamentos que possibilitam as execuções das coletas
        df_instancias['LaunchTime']          = df_instancias['LaunchTime']+timedelta(hours=-3)
  
        df_instancias['Data_encerramento'] = datetime.today()-timedelta(days=1)
        df_instancias['Data_encerramento'] = df_instancias['Data_encerramento'].astype('datetime64')

        df_instancias['Data_encerramento']   = df_instancias['Data_encerramento'].fillna(datetime.today().strftime('%Y-%m-%d 00:00:00'))

        #Define as metricas que serão consultadas nas instâncias
        lst_metricas = ['StatusCheckFailed', 'StatusCheckFailed_Instance', 'StatusCheckFailed_System']

        #Cria a lista com as datas para a execução das coletas
        lst_data = pd.date_range(date.today()-timedelta(days=1), date.today()-timedelta(days=1))

        pathcsv=pathAWS_TELcsv

        #Define o caminho para a execução dos backups das coletas
        caminho_csv = pathcsv + '{}/telemedicina_{}_{}_homologacao.csv'
        
        #Executa as coletas e transformações dos checks nas instâncias
        for dia in lst_data:
            #print(dia)
            print('Data de Coleta: {}!!!'.format(dia))
            st_date = format(dia, '%Y-%m-%d 00:00:00')
            ed_date = format(dia+timedelta(days=1), '%Y-%m-%d 00:00:00')
            
            st_date = datetime.strptime(st_date, '%Y-%m-%d %H:%M:%S')
            ed_date = datetime.strptime(ed_date, '%Y-%m-%d %H:%M:%S')
            
            for metrica in lst_metricas:
                dados_check = pd.DataFrame()
                dados_indis = pd.DataFrame()
                
                for index, instancia in df_instancias.iterrows():
                    response_intarator = client_homol.get_metric_data(
                        MetricDataQueries=[
                            {
                                'Id': 'e1',
                                'MetricStat': {
                                    'Metric': {
                                        'MetricName': '{}'.format(metrica),
                                        'Namespace': 'AWS/EC2',
                                        'Dimensions': [
                                            {
                                                'Name': 'InstanceId',
                                                'Value': '{}'.format(instancia['InstanceId'])
                                            },
                                        ]
                                    },
                                    'Period': 60,
                                    'Stat': 'Maximum'
                                }
                            }
                        ],
                        StartTime=st_date,
                        EndTime=ed_date)

                    Datas = pd.json_normalize(pd.json_normalize(pd.json_normalize(response_intarator)['MetricDataResults'])[0])['Timestamps']
                    Values = pd.json_normalize(pd.json_normalize(pd.json_normalize(response_intarator)['MetricDataResults'])[0])['Values'][0]
                    a=0
                    name = metrica
                    name = pd.DataFrame()

                    for Data in Datas[0]:
                        name.loc[a, ['Data', 'Status']] = Data, Values[a]
                        a+=1

                    dados_filtro = pd.DataFrame()

                    if len(name)>0:
                        name.Data                 = name.Data.astype('datetime64')
                        name['Metrica']           = metrica
                        name['Resource']          = 'AWS/EC2'
                        name['Instancia']         = instancia['InstanceId']
                        name['ID_metrica']        = 'InstanceId'
                        name['ambiente']          = 'homologacao'
                        name['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        name['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        name['StopReason']        = instancia['StateTransitionReason']
                        name['State']             = instancia['State']
                        name['KeyName']           = instancia['KeyName']
                        name['region']            = 'sa-east-1'
                        
                        min_time = st_date
                        max_time = ed_date

                        date_list             = pd.DataFrame()
                        date_list['Data']     = pd.date_range(min_time.date(), max_time.date(), freq= 'min')[:-1]
                        date_list['Status_1'] = 1

                        dados_filtro = name.set_index('Data').join(date_list.set_index('Data'), how='right').reset_index()

                        dados_filtro = dados_filtro[dados_filtro['Status'].isna()]

                        dados_filtro['Status']            = 2
                        dados_filtro['Resource']          = 'AWS/EC2'
                        dados_filtro['Instancia']         = instancia['InstanceId']
                        dados_filtro['Metrica']           = metrica
                        dados_filtro['ID_metrica']        = 'InstanceId'
                        dados_filtro['ambiente']          = 'homologacao'
                        dados_filtro['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['StopReason']        = instancia['StateTransitionReason']
                        dados_filtro['State']             = instancia['State']
                        dados_filtro['KeyName']           = instancia['KeyName']
                        dados_filtro['region']            = 'sa-east-1'

                        dados_filtro = dados_filtro[['Data','Status','Metrica','Resource','Instancia',
                                                    'ID_metrica','ambiente','Data_criacao','Data_encerramento',
                                                    'StopReason','State', 'KeyName', 'region']]
                        
                        
                    elif(len(name)==0 and dia.date()>=instancia['LaunchTime'].date()):
                        min_time = st_date
                        max_time = ed_date

                        date_list             = pd.DataFrame()
                        date_list['Data']     = pd.date_range(min_time.date(), max_time.date(), freq= 'min')[:-1]
                        date_list['Status_1'] = 1     

                        dados_filtro = date_list

                        dados_filtro['Status']            = 2
                        dados_filtro['Resource']          = 'AWS/EC2'
                        dados_filtro['Instancia']         = instancia['InstanceId']
                        dados_filtro['Metrica']           = metrica
                        dados_filtro['ID_metrica']        = 'InstanceId'
                        dados_filtro['ambiente']          = 'homologacao'
                        dados_filtro['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['StopReason']        = instancia['StateTransitionReason']
                        dados_filtro['State']             = instancia['State']
                        dados_filtro['KeyName']           = instancia['KeyName']
                        dados_filtro['region']            = 'sa-east-1'

                        dados_filtro = dados_filtro[['Data','Status','Metrica','Resource','Instancia',
                                                    'ID_metrica','ambiente','Data_criacao','Data_encerramento',
                                                    'StopReason','State', 'KeyName', 'region']]

                        
                    dados_check = dados_check.append(name)
                    dados_indis = dados_indis.append(dados_filtro)
                    
                #Executa o backup caso a execução seja realizada na VM
                if(backup==True):
                    pathcsvbk=(pathcsv+'{}/').format(metrica.lower())
                    pathcsvbk = oGet_ClassApoio.Criar_Path(str(pathcsvbk))
                    dados_check.to_csv(caminho_csv.format(metrica.lower(), metrica.lower(), Data.strftime('%d%m%Y'))
                                                            , index=False, sep=";", quoting=1)
                
                oGet_DbConnect.inserir_dados(dados_check, 'tb_AWS_{}'.format(metrica))

                #Executa o backup caso a execução seja realizada na VM
                if(backup==True):
                    pathcsvbk=(pathcsv+'{}/').format(metrica.lower())
                    
                    dados_indis.to_csv(caminho_csv.format(metrica.lower(), metrica.lower(), Data.strftime('indisponibilidade_%d%m%Y'))
                                        , index=False, sep=";", quoting=1)

                oGet_DbConnect.inserir_dados(dados_indis, 'tb_AWS_{}_indisponibilidade'.format(metrica))

        return dia
    
    def Conexao_CloudWatch_Desenvolve_SP(self,pathAWS_DESENVSPcsv,backup,verify,filepath_chaves_aws):

        oGet_ClassApoio = Get_ClassApoio()
        oGet_DbConnect = Get_DbConnect()       

        #Conecta aos clients da AWS de Telemedicina nos ambientes de produção
        #Client de produção
        client_prod = oGet_ClassApoio.Connect_AWS_Client(filepath_chaves_aws,'metricas', 'desenvolvesp', 'producao'  , verify)

        #Consulta dos dados das instâncias no SQL Server
        instancias = oGet_DbConnect.consulta_dados('tb_aws_check_status_instancias_desenvolve_sp')
        
        #Define o intervalo de execução das coletas
        datas = datetime.today()-timedelta(days=1)

        #Definições iniciais das instâncias para a execução dos scripts
        df_instancias = instancias[(instancias['StateTransitionReason']=='') | 
                (instancias['StateTransitionReason'].str.replace('User initiated \(', '').str.replace(' GMT\)', '').str.replace('User initiated', '')>=datas.strftime('%Y-%m-%d 00:00:00'))]
      
        #Executa alguns tratamentos que possibilitam as execuções das coletas
        df_instancias['LaunchTime']          = df_instancias['LaunchTime']+timedelta(hours=-3)
     
        df_instancias['StateTransitionReason']=df_instancias['StateTransitionReason'].str.replace(' GMT\)', '').str.split('(', expand=True)
        df_instancias['Data_encerramento'] = datetime.today()-timedelta(days=15)
        df_instancias['Data_encerramento'] = df_instancias['Data_encerramento'].astype('datetime64')
        
        #Define as metricas que serão consultadas nas instâncias
        lst_metricas = ['StatusCheckFailed', 'StatusCheckFailed_Instance', 'StatusCheckFailed_System']

        #Cria a lista com as datas para a execução das coletas
        lst_data = pd.date_range(date.today()-timedelta(days=1), date.today()-timedelta(days=1))

        pathcsv=pathAWS_DESENVSPcsv
        
        #Define o caminho para a execução dos backups das coletas
        caminho_csv = pathcsv + '{}/desenvolvesp_{}_{}.csv'
    
        #Executa as coletas e transformações dos checks nas instâncias
        for dia in lst_data:
            #print(dia)
            print('Data de Coleta: {}!!!'.format(dia))
            st_date = format(dia, '%Y-%m-%d 00:00:00')
            ed_date = format(dia+timedelta(days=1), '%Y-%m-%d 00:00:00')
            
            st_date = datetime.strptime(st_date, '%Y-%m-%d %H:%M:%S')
            ed_date = datetime.strptime(ed_date, '%Y-%m-%d %H:%M:%S')
            
            for metrica in lst_metricas:
                dados_check = pd.DataFrame()
                dados_indis = pd.DataFrame()
                
                for index, instancia in df_instancias.iterrows():
                    response_intarator = client_prod.get_metric_data(
                        MetricDataQueries=[
                            {
                                'Id': 'e1',
                                'MetricStat': {
                                    'Metric': {
                                        'MetricName': '{}'.format(metrica),
                                        'Namespace': 'AWS/EC2',
                                        'Dimensions': [
                                            {
                                                'Name': 'InstanceId',
                                                'Value': '{}'.format(instancia['InstanceId'])
                                            },
                                        ]
                                    },
                                    'Period': 60,
                                    'Stat': 'Maximum'
                                }
                            }
                        ],
                        StartTime=st_date,
                        EndTime=ed_date)

                    Datas = pd.json_normalize(pd.json_normalize(pd.json_normalize(response_intarator)['MetricDataResults'])[0])['Timestamps']
                    Values = pd.json_normalize(pd.json_normalize(pd.json_normalize(response_intarator)['MetricDataResults'])[0])['Values'][0]
                    a=0
                    name = metrica
                    name = pd.DataFrame()

                    for Data in Datas[0]:
                        name.loc[a, ['Data', 'Status']] = Data, Values[a]
                        a+=1

                    dados_filtro = pd.DataFrame()

                    if len(name)>0:
                        name.Data                 = name.Data.astype('datetime64')
                        name['Metrica']           = metrica
                        name['Resource']          = 'AWS/EC2'
                        name['Instancia']         = instancia['InstanceId']
                        name['ID_metrica']        = 'InstanceId'
                        name['ambiente']          = 'producao'
                        name['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        name['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        name['StopReason']        = instancia['StateTransitionReason']
                        name['State']             = instancia['State']
                        name['KeyName']           = instancia['KeyName']
                        name['region']            = 'sa-east-1'
                        
                        #name.to_csv(caminho_csv.format(metrica+'_'+instancia['InstanceId']+Data.strftime('_%d%m%Y'))
                        #                , index=False, sep=";", quoting=1)
                        
                        #input_AWS_desenvolvesp(metrica, name)

                        min_time = st_date
                        max_time = ed_date

                        date_list             = pd.DataFrame()
                        date_list['Data']     = pd.date_range(min_time.date(), max_time.date(), freq= 'min')[:-1]
                        date_list['Status_1'] = 1

                        dados_filtro = name.set_index('Data').join(date_list.set_index('Data'), how='right').reset_index()

                        dados_filtro = dados_filtro[dados_filtro['Status'].isna()]

                        dados_filtro['Status']            = 2
                        dados_filtro['Resource']          = 'AWS/EC2'
                        dados_filtro['Instancia']         = instancia['InstanceId']
                        dados_filtro['Metrica']           = metrica
                        dados_filtro['ID_metrica']        = 'InstanceId'
                        dados_filtro['ambiente']          = 'producao'
                        dados_filtro['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['StopReason']        = instancia['StateTransitionReason']
                        dados_filtro['State']             = instancia['State']
                        dados_filtro['KeyName']           = instancia['KeyName']
                        dados_filtro['region']            = 'sa-east-1'
                        
                        dados_filtro = dados_filtro[['Data','Status','Metrica','Resource','Instancia',
                                                    'ID_metrica','ambiente','Data_criacao','Data_encerramento',
                                                    'StopReason','State', 'KeyName', 'region']]                        
                        
                    elif(len(name)==0 and dia.date()>=instancia['LaunchTime'].date()):
                        min_time = st_date
                        max_time = ed_date

                        date_list             = pd.DataFrame()
                        date_list['Data']     = pd.date_range(min_time.date(), max_time.date(), freq= 'min')[:-1]
                        date_list['Status_1'] = 1     

                        dados_filtro = date_list

                        dados_filtro['Status']            = 2
                        dados_filtro['Resource']          = 'AWS/EC2'
                        dados_filtro['Instancia']         = instancia['InstanceId']
                        dados_filtro['Metrica']           = metrica
                        dados_filtro['ID_metrica']        = 'InstanceId'
                        dados_filtro['ambiente']          = 'producao'
                        dados_filtro['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['StopReason']        = instancia['StateTransitionReason']
                        dados_filtro['State']             = instancia['State']
                        dados_filtro['KeyName']           = instancia['KeyName']
                        dados_filtro['region']            = 'sa-east-1'
                        
                        dados_filtro = dados_filtro[['Data','Status','Metrica','Resource','Instancia',
                                                    'ID_metrica','ambiente','Data_criacao','Data_encerramento',
                                                    'StopReason','State', 'KeyName', 'region']]
                        
                    dados_check = dados_check.append(name)
                    dados_indis = dados_indis.append(dados_filtro)
                    
                #Executa o backup caso a execução seja realizada na VM
                if(backup==True):
                    pathcsvbk=(pathcsv+'{}/').format(metrica.lower())
                    #print(pathcsv)
                    pathcsvbk = oGet_ClassApoio.Criar_Path(str(pathcsvbk))
                    dados_check.to_csv(caminho_csv.format(metrica.lower(), metrica.lower(), Data.strftime('%d%m%Y'))
                                                            , index=False, sep=";", quoting=1)
                oGet_DbConnect.inserir_dados(dados_check, 'tb_AWS_{}_desenvolvesp'.format(metrica))

                #Executa o backup caso a execução seja realizada na VM
                if(backup==True):
                    pathcsvbk=(pathcsv+'{}/').format(metrica.lower())
                    #print(pathcsv)
                    pathcsvbk = oGet_ClassApoio.Criar_Path(str(pathcsvbk))
                    dados_indis.to_csv(caminho_csv.format(metrica.lower(), metrica.lower(), Data.strftime('indisponibilidade_%d%m%Y'))
                                        , index=False, sep=";", quoting=1)

                oGet_DbConnect.inserir_dados(dados_indis, 'tb_AWS_{}_desenvolvesp_indisponibilidade'.format(metrica))

        return dia
    
    def Conexao_CloudWatch_CPU_homol(self,pathAWS_TELcsv,backup,verify,filepath_chaves_aws):

        oGet_ClassApoio = Get_ClassApoio()
        oGet_DbConnect = Get_DbConnect()       

        #Conecta aos clients da AWS de Telemedicina nos ambientes de produção
        #Client de produção
        client_homol = oGet_ClassApoio.Connect_AWS_Client(filepath_chaves_aws,'metricas', 'telemedicina', 'homologacao'  , verify)

        #Consulta dos dados das instâncias no SQL Server
        instancias = oGet_DbConnect.consulta_dados('tb_aws_check_status_instancias_telemedicina', filtro=" WHERE ambiente='homologacao'")
        
        #Define o intervalo de execução das coletas
        datas = datetime.today()-timedelta(days=1)

        #Definições iniciais das instâncias para a execução dos scripts
        df_instancias = instancias[(instancias['StateTransitionReason']=='') | 
                (instancias['StateTransitionReason'].str.replace('User initiated \(', '').str.replace(' GMT\)', '').str.replace('User initiated', '')>=datas.strftime('%Y-%m-%d 00:00:00'))]
       

        #Executa alguns tratamentos que possibilitam as execuções das coletas
        df_instancias['LaunchTime']          = df_instancias['LaunchTime']+timedelta(hours=-3)
        

        #df_instancias['StateTransitionReason']=df_instancias['StateTransitionReason'].str.replace(' GMT\)', '').str.split('(', expand=True) 
        df_instancias['Data_encerramento'] = datetime.today()-timedelta(days=15)
        df_instancias['Data_encerramento'] = df_instancias['Data_encerramento'].astype('datetime64')

        
        df_instancias['Data_encerramento']   = df_instancias['Data_encerramento'].fillna(datetime.today().strftime('%Y-%m-%d 00:00:00'))

        #Define as metricas que serão consultadas nas instâncias
        lst_metricas = ['CPUUtilization']

        #Cria a lista com as datas para a execução das coletas
        lst_data = pd.date_range(date.today()-timedelta(days=1), date.today()-timedelta(days=1))

        pathcsv=pathAWS_TELcsv  

        #Define o caminho para a execução dos backups das coletas
        #caminho_csv = 'D:/Metricas/Copia_seguranca/AWS_API/TELEMEDICINA/{}/telemedicina_{}_{}_homologacao.csv'
        caminho_csv = pathcsv + '{}/telemedicina_{}_{}_homologacao.csv'

        #Executa as coletas e transformações dos checks nas instâncias
        for dia in lst_data:
            #print(dia)
            print('Data de Coleta: {}!!!'.format(dia))
            st_date = format(dia, '%Y-%m-%d 00:00:00')
            ed_date = format(dia+timedelta(days=1), '%Y-%m-%d 00:00:00')
            
            st_date = datetime.strptime(st_date, '%Y-%m-%d %H:%M:%S')
            ed_date = datetime.strptime(ed_date, '%Y-%m-%d %H:%M:%S')
            
            for metrica in lst_metricas:
                dados_check = pd.DataFrame()
                dados_indis = pd.DataFrame()
                
                for index, instancia in df_instancias.iterrows():
                    response_intarator = client_homol.get_metric_data(
                        MetricDataQueries=[
                            {
                                'Id': 'e1',
                                'MetricStat': {
                                    'Metric': {
                                        'MetricName': '{}'.format(metrica),
                                        'Namespace': 'AWS/EC2',
                                        'Dimensions': [
                                            {
                                                'Name': 'InstanceId',
                                                'Value': '{}'.format(instancia['InstanceId'])
                                            },
                                        ]
                                    },
                                    'Period': 60,
                                    'Stat': 'Maximum'
                                }
                            }
                        ],
                        StartTime=st_date,
                        EndTime=ed_date)

                    Datas = pd.json_normalize(pd.json_normalize(pd.json_normalize(response_intarator)['MetricDataResults'])[0])['Timestamps']
                    Values = pd.json_normalize(pd.json_normalize(pd.json_normalize(response_intarator)['MetricDataResults'])[0])['Values'][0]
                    a=0
                    name = metrica
                    name = pd.DataFrame()

                    for Data in Datas[0]:
                        name.loc[a, ['Data', 'Status']] = Data, Values[a]
                        a+=1

                    dados_filtro = pd.DataFrame()

                    if len(name)>0:
                        name.Data                 = name.Data.astype('datetime64')
                        name['Metrica']           = metrica
                        name['Resource']          = 'AWS/EC2'
                        name['Instancia']         = instancia['InstanceId']
                        name['ID_metrica']        = 'InstanceId'
                        name['ambiente']          = 'homologacao'
                        name['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        name['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        name['StopReason']        = instancia['StateTransitionReason']
                        name['State']             = instancia['State']
                        name['KeyName']           = instancia['KeyName']
                        name['region']            = 'sa-east-1'
                        
                        

                        min_time = st_date
                        max_time = ed_date

                        date_list             = pd.DataFrame()
                        date_list['Data']     = pd.date_range(min_time.date(), max_time.date(), freq= 'min')[:-1]
                        date_list['Status_1'] = 1

                        dados_filtro = name.set_index('Data').join(date_list.set_index('Data'), how='right').reset_index()

                        dados_filtro = dados_filtro[dados_filtro['Status'].isna()]

                        dados_filtro['Status']            = 0
                        dados_filtro['Resource']          = 'AWS/EC2'
                        dados_filtro['Instancia']         = instancia['InstanceId']
                        dados_filtro['Metrica']           = metrica
                        dados_filtro['ID_metrica']        = 'InstanceId'
                        dados_filtro['ambiente']          = 'homologacao'
                        dados_filtro['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['StopReason']        = instancia['StateTransitionReason']
                        dados_filtro['State']             = instancia['State']
                        dados_filtro['KeyName']           = instancia['KeyName']
                        dados_filtro['region']            = 'sa-east-1'

                        dados_filtro = dados_filtro[['Data','Status','Metrica','Resource','Instancia',
                                                    'ID_metrica','ambiente','Data_criacao','Data_encerramento',
                                                    'StopReason','State', 'KeyName', 'region']]
                        
                        
                    elif(len(name)==0 and dia.date()>=instancia['LaunchTime'].date()):
                        min_time = st_date
                        max_time = ed_date

                        date_list             = pd.DataFrame()
                        date_list['Data']     = pd.date_range(min_time.date(), max_time.date(), freq= 'min')[:-1]
                        date_list['Status_1'] = 1     

                        dados_filtro = date_list

                        dados_filtro['Status']            = 0
                        dados_filtro['Resource']          = 'AWS/EC2'
                        dados_filtro['Instancia']         = instancia['InstanceId']
                        dados_filtro['Metrica']           = metrica
                        dados_filtro['ID_metrica']        = 'InstanceId'
                        dados_filtro['ambiente']          = 'homologacao'
                        dados_filtro['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['StopReason']        = instancia['StateTransitionReason']
                        dados_filtro['State']             = instancia['State']
                        dados_filtro['KeyName']           = instancia['KeyName']
                        dados_filtro['region']            = 'sa-east-1'

                        dados_filtro = dados_filtro[['Data','Status','Metrica','Resource','Instancia',
                                                    'ID_metrica','ambiente','Data_criacao','Data_encerramento',
                                                    'StopReason','State', 'KeyName', 'region']]

                        
                    dados_check = dados_check.append(name)
                    dados_indis = dados_indis.append(dados_filtro)
                    
                #Executa o backup caso a execução seja realizada na VM
                if(backup==True):
                    pathcsvbk=(pathcsv+'{}/').format(metrica.lower())
                    #print(pathcsv)
                    pathcsvbk = oGet_ClassApoio.Criar_Path(str(pathcsvbk))
                    dados_check.to_csv(caminho_csv.format(metrica.lower(), metrica.lower(), Data.strftime('%d%m%Y'))
                                                            , index=False, sep=";", quoting=1)
                oGet_DbConnect.inserir_dados(dados_check, 'tb_AWS_{}'.format(metrica))

                #Executa o backup caso a execução seja realizada na VM
                if(backup==True):
                    pathcsvbk=(pathcsv+'{}/').format(metrica.lower())
                    #print(pathcsv)
                    pathcsvbk = oGet_ClassApoio.Criar_Path(str(pathcsvbk))
                    dados_indis.to_csv(caminho_csv.format(metrica.lower(), metrica.lower(), Data.strftime('indisponibilidade_%d%m%Y'))
                                        , index=False, sep=";", quoting=1)

                oGet_DbConnect.inserir_dados(dados_indis, 'tb_AWS_{}_indisponibilidade'.format(metrica))

        return dia
    
    def Conexao_CloudWatch_CPU_prod(self,pathAWS_TELcsv,backup,verify,filepath_chaves_aws):

        oGet_ClassApoio = Get_ClassApoio()
        oGet_DbConnect = Get_DbConnect()       

        #Conecta aos clients da AWS de Telemedicina nos ambientes de produção
        #Client de produção
        client_prod = oGet_ClassApoio.Connect_AWS_Client(filepath_chaves_aws,'metricas', 'telemedicina', 'producao'  , verify)

        #Consulta dos dados das instâncias no SQL Server
        instancias = oGet_DbConnect.consulta_dados('tb_aws_check_status_instancias_telemedicina', filtro=" WHERE ambiente='producao'")

        #Define o intervalo de execução das coletas
        datas = datetime.today()-timedelta(days=1)
        
        #Definições iniciais das instâncias para a execução dos scripts
        df_instancias = instancias[(instancias['StateTransitionReason']=='') | 
                (instancias['StateTransitionReason'].str.replace('User initiated \(', '').str.replace(' GMT\)', '').str.replace('User initiated', '')>=datas.strftime('%Y-%m-%d 00:00:00'))]

        #Executa alguns tratamentos que possibilitam as execuções das coletas
        df_instancias['LaunchTime']          = df_instancias['LaunchTime']+timedelta(hours=-3)

        df_instancias['StateTransitionReason']=df_instancias['StateTransitionReason'].str.replace(' GMT\)', '').str.split('(', expand=True)
        df_instancias['Data_encerramento']   = datetime.today()-timedelta(days=15)
        df_instancias['Data_encerramento']   = df_instancias['Data_encerramento'].astype('datetime64')        
        df_instancias['Data_encerramento']   = df_instancias['Data_encerramento'].fillna(datetime.today().strftime('%Y-%m-%d 00:00:00'))

        #Define as metricas que serão consultadas nas instãncias
        lst_metricas = ['CPUUtilization']

        #Cria a lista com as datas para a execução das coletas
        lst_data = pd.date_range(date.today()-timedelta(days=1), date.today()-timedelta(days=1))

        pathcsv=pathAWS_TELcsv   

        #Define o caminho para a execução dos backups das coletas       
        caminho_csv = pathcsv + '{}/telemedicina_{}_{}_producao.csv'

        #Executa as coletas e transformações dos checks nas instâncias
        for dia in lst_data:
            #print(dia)
            print('Data de Coleta: {}!!!'.format(dia))
            st_date = format(dia, '%Y-%m-%d 00:00:00')
            ed_date = format(dia+timedelta(days=1), '%Y-%m-%d 00:00:00')
            
            st_date = datetime.strptime(st_date, '%Y-%m-%d %H:%M:%S')
            ed_date = datetime.strptime(ed_date, '%Y-%m-%d %H:%M:%S')
            
            for metrica in lst_metricas:
                dados_check = pd.DataFrame()
                dados_indis = pd.DataFrame()
                
                for index, instancia in df_instancias.iterrows():
                    response_intarator = client_prod.get_metric_data(
                        MetricDataQueries=[
                            {
                                'Id': 'e1',
                                'MetricStat': {
                                    'Metric': {
                                        'MetricName': '{}'.format(metrica),
                                        'Namespace': 'AWS/EC2',
                                        'Dimensions': [
                                            {
                                                'Name': 'InstanceId',
                                                'Value': '{}'.format(instancia['InstanceId'])
                                            },
                                        ]
                                    },
                                    'Period': 60,
                                    'Stat': 'Maximum'
                                }
                            }
                        ],
                        StartTime=st_date,
                        EndTime=ed_date)

                    Datas = pd.json_normalize(pd.json_normalize(pd.json_normalize(response_intarator)['MetricDataResults'])[0])['Timestamps']
                    Values = pd.json_normalize(pd.json_normalize(pd.json_normalize(response_intarator)['MetricDataResults'])[0])['Values'][0]
                    a=0
                    name = metrica
                    name = pd.DataFrame()

                    for Data in Datas[0]:
                        name.loc[a, ['Data', 'Status']] = Data, Values[a]
                        a+=1

                    dados_filtro = pd.DataFrame()

                    if len(name)>0:
                        name.Data                 = name.Data.astype('datetime64')
                        name['Metrica']           = metrica
                        name['Resource']          = 'AWS/EC2'
                        name['Instancia']         = instancia['InstanceId']
                        name['ID_metrica']        = 'InstanceId'
                        name['ambiente']          = 'producao'
                        name['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        name['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        name['StopReason']        = instancia['StateTransitionReason']
                        name['State']             = instancia['State']
                        name['KeyName']           = instancia['KeyName']
                        name['region']            = 'sa-east-1'
                                               

                        min_time = st_date
                        max_time = ed_date

                        date_list             = pd.DataFrame()
                        date_list['Data']     = pd.date_range(min_time.date(), max_time.date(), freq= 'min')[:-1]
                        date_list['Status_1'] = 1

                        dados_filtro = name.set_index('Data').join(date_list.set_index('Data'), how='right').reset_index()

                        dados_filtro = dados_filtro[dados_filtro['Status'].isna()]

                        dados_filtro['Status']            = 0
                        dados_filtro['Resource']          = 'AWS/EC2'
                        dados_filtro['Instancia']         = instancia['InstanceId']
                        dados_filtro['Metrica']           = metrica
                        dados_filtro['ID_metrica']        = 'InstanceId'
                        dados_filtro['ambiente']          = 'producao'
                        dados_filtro['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['StopReason']        = instancia['StateTransitionReason']
                        dados_filtro['State']             = instancia['State']
                        dados_filtro['KeyName']           = instancia['KeyName']
                        dados_filtro['region']            = 'sa-east-1'

                        dados_filtro = dados_filtro[['Data','Status','Metrica','Resource','Instancia',
                                                    'ID_metrica','ambiente','Data_criacao','Data_encerramento',
                                                    'StopReason','State', 'KeyName', 'region']]
                        
                        
                    elif(len(name)==0 and dia.date()>=instancia['LaunchTime'].date()):
                        min_time = st_date
                        max_time = ed_date

                        date_list             = pd.DataFrame()
                        date_list['Data']     = pd.date_range(min_time.date(), max_time.date(), freq= 'min')[:-1]
                        date_list['Status_1'] = 1     

                        dados_filtro = date_list

                        dados_filtro['Status']            = 0
                        dados_filtro['Resource']          = 'AWS/EC2'
                        dados_filtro['Instancia']         = instancia['InstanceId']
                        dados_filtro['Metrica']           = metrica
                        dados_filtro['ID_metrica']        = 'InstanceId'
                        dados_filtro['ambiente']          = 'producao'
                        dados_filtro['Data_criacao']      = instancia['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['Data_encerramento'] = instancia['Data_encerramento'].strftime('%Y-%m-%d %H:%M:%S')
                        dados_filtro['StopReason']        = instancia['StateTransitionReason']
                        dados_filtro['State']             = instancia['State']
                        dados_filtro['KeyName']           = instancia['KeyName']
                        dados_filtro['region']            = 'sa-east-1'

                        dados_filtro = dados_filtro[['Data','Status','Metrica','Resource','Instancia',
                                                    'ID_metrica','ambiente','Data_criacao','Data_encerramento',
                                                    'StopReason','State', 'KeyName', 'region']]

                        
                    dados_check = dados_check.append(name)
                    dados_indis = dados_indis.append(dados_filtro)
                    
                #Executa o backup caso a execução seja realizada na VM
                if(backup==True):
                    pathcsvbk=(pathcsv+'{}/').format(metrica.lower())
                    #print(pathcsv)
                    pathcsvbk = oGet_ClassApoio.Criar_Path(str(pathcsvbk))
                    dados_check.to_csv(caminho_csv.format(metrica.lower(), metrica.lower(), Data.strftime('%d%m%Y'))
                                                            , index=False, sep=";", quoting=1)
                
                oGet_DbConnect.inserir_dados(dados_check, 'tb_AWS_{}'.format(metrica))

                #Executa o backup caso a execução seja realizada na VM
                if(backup==True):
                    pathcsvbk=(pathcsv+'{}/').format(metrica.lower())
                    #print(pathcsv)
                    pathcsvbk = oGet_ClassApoio.Criar_Path(str(pathcsvbk))
                    dados_indis.to_csv(caminho_csv.format(metrica.lower(), metrica.lower(), Data.strftime('indisponibilidade_%d%m%Y'))
                                        , index=False, sep=";", quoting=1)

                oGet_DbConnect.inserir_dados(dados_indis, 'tb_AWS_{}_indisponibilidade'.format(metrica))

        return dia
