import logging
from datetime import datetime
import pyodbc
import numpy as np
import pandas as pd
from glob import glob
import warnings
warnings.filterwarnings("ignore")

logging.basicConfig(filename="01-Atividades_CSFN_log.txt",
                level=logging.INFO,
                format='%(levelname)s: %(asctime)s %(message)s',
                datefmt='%d/%m/%y %I:%M:%S')

def logmessage(msg):
    logging.info( msg)
    print(datetime.now().strftime("%d-%m-%Y %H:%M:%S") + ' | ' +msg)

class banco():
    def acesso(self):
        logmessage('Capturando acessos')
        self.servidor = '10.200.163.14'
        self.bd_homologacao = 'Dbpslmprodespdc'
        self.bd_teste = 'd-slmProdespdc'
        self.login = 'adm-antonioalmeida'
        self.senha = 'gn$!2-hu78'
        logmessage('Acessos capturados')

    def conectar(self):
        logmessage('Realizando conexão com o banco de dados')
        self.conect = pyodbc.connect('Driver={SQL Server};Server='+self.servidor+';Database='+self.bd_homologacao+';UID='+self.login+';PWD='+self.senha) 
        self.cursor = self.conect.cursor();logmessage('Conexão realizada')
        
        #print('Conexão realizada')

    def desconectar(self):
        logmessage('Realizando desconexão com o banco de dados')
        self.desconect = self.cursor.close();logmessage('Conexão encarrada')

    def aplicar(self):
        self.cursor.commit()
    
    def deletar(self):
        logmessage('Deletando informações da tabela')
        self.deletar_tabela = self.cursor.execute ('''DROP TABLE IF EXISTS tt_atividades_csfn 
                                                      CREATE table tt_atividades_csfn
                                                      (
                                                       id	NVARCHAR(50),
                                                       Tarefa	NVARCHAR(500),
                                                       Bucket	NVARCHAR(60),
                                                       Progresso	NVARCHAR(20),
                                                       Prioridade	NVARCHAR(10),
                                                       atribuido_a	NVARCHAR(400),
                                                       Criado_por	NVARCHAR(70),
                                                       Criado_em	NVARCHAR(10),
                                                       Data_de_inicio	NVARCHAR(10),
                                                       Data_de_conclusao	NVARCHAR(10),
                                                       Atrasados	NVARCHAR(15),
                                                       Concluido_em	NVARCHAR(10),
                                                       Concluida_por	NVARCHAR(70),
                                                       itens_da_lista	NVARCHAR(10),
                                                       Rotulo	NVARCHAR(50))''')
        self.cursor.commit()
        logmessage('Informações deletadas da tabela')

    def inserir(self):
        logmessage('Inserindo novas informações na tabela temporaria')
        for row in self.df4.itertuples():
           self.cursor.execute('''
           insert into tt_atividades_csfn (
           id,	Tarefa,Bucket,Progresso,Prioridade,atribuido_a,Criado_por,Criado_em,Data_de_inicio,Data_de_conclusao,Atrasados,Concluido_em,Concluida_por,itens_da_lista,Rotulo) 
           values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
           ,row.id,row.Tarefa,row.Bucket,row.Progresso,row.Prioridade,row.atribuido_a,row.Criado_por,row.Criado_em,
           row.inicio,row.Data_de_conclusao,row.Atrasados,row.Concluido_em,row.Concluida_por,row.itens_da_lista,row.Rotulo)
        self.cursor.commit()
        logmessage('Informações inseridas')

        logmessage('Alimentando informações da tabela temporaria na tabela de controle de input')
        self.cursor.execute(''' 
        insert into tb_controle_input
        select tabela.*,information.colunas          from (select getdate() data_input,'tt_atividades_csfn' table_name,count(*)qtd_registros from tt_atividades_csfn) tabela
        left join (select table_name,count(*)colunas from information_schema.COLUMNS where table_name = 'tt_atividades_csfn' group by table_name) information
        on information.TABLE_NAME = tabela.table_name
        
        ''')
        self.cursor.commit()
        logmessage('Informações inseridas')

        logmessage('Analisando registros antigos')
        self.cursor.execute('''
             delete from tb_atividades_csfn where id in
             (select distinct a.id from tb_atividades_csfn a
             inner join tt_atividades_csfn b on b.id = a.id)
             ''')
        self.cursor.commit()
        logmessage('Registros analisados')


        logmessage('Inserindo novas informações na tabela')
        self.cursor.execute(''' insert into tb_atividades_csfn 
        select
              Data,id,Tarefa,Bucket,Progresso,Prioridade	,value	atribuido_a,Criado_por,Criado_em,Data_de_inicio,Data_de_conclusao,Atrasados	
             ,Concluido_em,Concluida_por,itens_da_lista,Rotulo,RN,Concluido,Em_andamento,Nao_iniciado	
        ,case 
             when format(Data,'Mdd') between 121 and 220 then 'Fevereiro' 
             when format(Data,'Mdd') between 221 and 320 then 'Março' 
             when format(Data,'Mdd') between 321 and 420 then 'Abril' 
             when format(Data,'Mdd') between 421 and 520 then 'Maio' 
             when format(Data,'Mdd') between 521 and 620 then 'Junho' 
             when format(Data,'Mdd') between 621 and 720 then 'Julho' 
             when format(Data,'Mdd') between 721 and 820 then 'Agosto' 
             when format(Data,'Mdd') between 821 and 920 then 'Setembro' 
             when format(Data,'Mdd') between 921 and 1020 then 'Outubro' 
             when format(Data,'Mdd') between 1021 and 1120 then 'Novembro' 
             when format(Data,'Mdd') between 1121 and 1220 then 'Dezembro' 
             when format(Data,'Mdd') between 1221 and 1231 then 'Janeiro' 
             when format(Data,'Mdd') between 101 and 120 then 'Janeiro' else null end Mes_faturamento
        ,case 
             when format(Data,'Mdd') between 121 and 220 then 2
             when format(Data,'Mdd') between 221 and 320 then 3 
             when format(Data,'Mdd') between 321 and 420 then 4
             when format(Data,'Mdd') between 421 and 520 then 5 
             when format(Data,'Mdd') between 521 and 620 then 6
             when format(Data,'Mdd') between 621 and 720 then 7
             when format(Data,'Mdd') between 721 and 820 then 8
             when format(Data,'Mdd') between 821 and 920 then 9
             when format(Data,'Mdd') between 921 and 1020 then 10 
             when format(Data,'Mdd') between 1021 and 1120 then 11 
             when format(Data,'Mdd') between 1121 and 1220 then 12 
             when format(Data,'Mdd') between 1221 and 1231 then 1 
             when format(Data,'Mdd') between 101 and 120 then 1 else null end Mes_num_faturamento
             ,case 
                  when format(Data,'Mdd') between 101 and 1220 then year(Data) 
                  when format(Data,'Mdd') between 1221 and 1231 then year(Data)+1 else null end Ano_faturamento
             
             from
             (select *
             ,case when Data_de_conclusao is not null then  Data_de_conclusao
                   when Concluido_em is not null then  Concluido_em
                   when Data_de_inicio is not null then  Data_de_inicio
                   when Criado_em is not null then  Criado_em else null end data
             
             ,case when Progresso = 'Concluída' then 1 else 0 end Concluido
             ,case when Progresso = 'Em andamento' then 1 else 0 end Em_andamento
             ,case when Progresso = 'Não iniciada' then 1 else 0 end Nao_iniciado
             from
             (select * from
             (select *,row_number() over(partition by id  order by progresso) RN from
             (select distinct id,Tarefa,Bucket,Progresso,Prioridade,atribuido_a,Criado_por,
             convert(date,Criado_em,103)Criado_em,
             convert(date,case when Data_de_inicio ='dd'then null else Data_de_inicio end,103) Data_de_inicio,
             convert(date,case when Data_de_conclusao ='dd'then null else Data_de_conclusao end,103) Data_de_conclusao,
             Atrasados,
             convert(date,case when Concluido_em ='dd'then null else Concluido_em end,103) Concluido_em,
             case when Concluida_por ='dd'then null else Concluida_por end Concluida_por,
             case when itens_da_lista ='dd'then null else itens_da_lista end itens_da_lista,
             case when Rotulo ='dd'then null else Rotulo end Rotulo
             from [dbo].[tt_atividades_csfn]) base1)base2 
                   where RN =1)base3
             CROSS APPLY STRING_SPLIT(atribuido_a, ';'))base4
                   where (case when atribuido_a <> Concluida_por then 0 else 1 end) =1
             DROP TABLE IF EXISTS tt_atividades_csfn       
             ''')
        
        self.cursor.commit()
        logmessage('Informações inseridas')

        logmessage('Alimentando informações da tabela na tabela de controle de input')

        self.cursor.execute(''' 
        insert into tb_controle_input
        select tabela.*,information.colunas          from (select getdate() data_input,'tb_atividades_csfn' table_name,count(*)qtd_registros from tb_atividades_csfn) tabela
        left join (select table_name,count(*)colunas from information_schema.COLUMNS where table_name = 'tb_atividades_csfn' group by table_name) information
        on information.TABLE_NAME = tabela.table_name
        ''')
        self.cursor.commit()
        logmessage('Informações inseridas')

class arquivo():
    
    def parametros(self):
        self.caminho = (r"C:\Users\36123863878\PRODESP\PRODESP - GSF - CSFN - General\Site Mooca\Templates Report Builder\Atividades\01 - Arquivos")
        self.nome_arquivo = '/Atividades CSFN*.xlsx'

    def ler(self):
        logmessage('Lendo arquivo de Atividades')
        self.lista_tabela = []
        for self.indice,self.tabela in enumerate(glob(self.caminho+self.nome_arquivo)):
            self.lista_tabela.append(pd.read_excel(self.tabela,header=0))
            self.lista_tabela[self.indice]['nome']  =self.tabela
        self.df = pd.concat(self.lista_tabela)
    
    def tratar(self):
        logmessage('Selecionando colunas do arquivo de Atividades')
        self.df2 = self.df.drop(['É Recorrente','Itens da lista de verificação','Descrição'],axis=1)

        logmessage('Renomeando colunas dos arquivos de Atividades')
        self.df3 = self.df.rename(columns={'Identificação da tarefa':'id','Nome da tarefa':'Tarefa','Nome do Bucket':'Bucket','Atribuído a':'atribuido_a',
                       'Criado por':'Criado_por','Criado em':'Criado_em','Data de início':'inicio',
                       'Data de conclusão':'Data_de_conclusao','Concluído em':'Concluido_em','Concluída por':'Concluida_por',
                       'Itens concluídos da lista de verificação':'itens_da_lista','Rótulos':'Rotulo'})
        
        logmessage('Substituindo espaços em branco')
        self.df4 = self.df3.replace({np.nan:"dd"})
        logmessage('Arquivo de Atividades pronto para inserção')

    def salvar(self):
        self.xlsx = self.df4.to_excel('teste.xlsx',index=False);print('Arquivo salvo com sucesso')

class processo(banco,arquivo):
    def __init__(self):
        logmessage('Iniciando processo de inserção do Atividades')
        self.acesso()
        self.conectar()
        self.parametros()
        self.ler()
        self.tratar()
        #self.salvar()
        self.deletar()
        self.inserir()
        self.desconectar()
        logmessage('Processo de inserção do Atividades concluido com sucesso')

processo()
