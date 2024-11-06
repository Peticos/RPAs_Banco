class RPA:
    # Construtor
    def __init__(self) -> None:
        # Importações
        import os
        from dotenv import load_dotenv
        from crud import Crud
        
        # A senha do Banco de Dados vem de um .env
        load_dotenv()
        password = os.getenv('PASSWORD12')
        
        # Dicionários de tabelas e suas colunas identificadoras
        self.ids_1 = {'administradores': 'id', 'dica_do_dia': 'id_dica', 'endereco': 'id_endereco', 'local': 'id_local', 'telefone_local': 'id_telefone_local', 'raca': 'id_raca', 'especie': 'id_especie'}
        self.ids_2 = {'admin': 'id_admin', 'day_hint': 'id_hint', 'address': 'id_address', 'locations': 'id_local', 'local_phone': 'id_local_phone', 'race': 'id_race', 'specie': 'id_specie'}
        
        # Dicionário com as tabelas do primeiro ano e suas equivalentes no do segundo ano
        self.tables = {'administradores': 'admin', 'dica_do_dia': 'day_hint', 'endereco': 'address', 'local': 'locations', 'telefone_local': 'local_phone', 'raca': 'race', 'especie': 'specie'}
        
        self.tables_2 = {'user_': 'usuario', 'pet_register': 'pet', 'weight': 'peso', 'vakinha': 'vakinha', 'vaccine': 'vacina', 'phone_user': 'telefone', 'rescued_lost': 'resgatado_perdido', 'doses': 'doses'}
        
        
        # Instância da classe CRUD, para ter acesso aos métodos 
        self.crud = Crud(*self.connect(password))
        
        # Dicionário com as funções de cada interação
        self.operations = {'INSERT': self.crud.insert, 'UPDATE': self.crud.update, 'DELETE': self.crud.delete}
        
    
    # Método de conexão com os bancos de dados
    def connect(self, password):
        '''
        ### Conecta com os dois bancos e retorna os cursores
        '''
        import psycopg2 as pg
        ## Banco do Primeiro Ano
        conn1 = pg.connect(
            dbname = "dbPeticos_1ano",
            user = "avnadmin",
            password = password,
            host = "db-peticos-cardosogih.k.aivencloud.com",
            port = 16207
        )
        self.cursor1 = conn1.cursor()

        ## Banco do Segundo ano
        conn2 = pg.connect(
            dbname = "dbPeticos_2ano",
            user = "avnadmin",
            password = password,
            host = "db-peticos-cardosogih.k.aivencloud.com",
            port = 16207
        )
        self.cursor2 = conn2.cursor()
        
        return self.cursor1, self.cursor2
    
    def atualizar_2(self) -> None:
        '''
        ### Método para enviar as atualizações do banco do primeiro ano para o do segundo
        '''
        # Encontrando as atualizações que ocorreram nos últimos 5 minutos
        self.cursor1.execute("SELECT * FROM log_total_1 WHERE alteration_date >= CURRENT_TIMESTAMP - INTERVAL '5 minutes';")
        modifications = self.cursor1.fetchall()
            
        if modifications != []:
            # Para cada linha da tabela, é executada a mesma alteração em sua tabela equivalente no banco do segundo ano
            for modification in modifications:
                alteration_date, id, table_name, operation, user = modification
                if table_name in self.tables.keys():
                    # Encontrando o nome da tabela equivalente no banco do segundo ano
                    table_name_2 = self.tables[table_name]
                    # Chamando o método da classe CRUD que executa a operação da linha que está na tabela de log
                    self.operations[str(operation).upper()](table_name, self.ids_1[table_name], table_name_2, self.ids_2[table_name_2], id, 1)
                
    def atualizar_1(self) -> None:
        '''
        ### Método para enviar as atualizações do banco do segundo ano para o do primeiro
        '''
        # Encontrando as atualizações que ocorreram nos últimos 5 minutos
        self.cursor2.execute("SELECT * FROM log_total_2 WHERE alteration_date >= CURRENT_TIMESTAMP - INTERVAL '5 minutes';")
        modifications = self.cursor1.fetchall()
            
        if modifications != []:
            # Para cada linha da tabela, é executada a mesma alteração em sua tabela equivalente no banco do segundo ano
            for modification in modifications:
                alteration_date, id, table_name, operation, user = modification
                if table_name in self.tables_2.keys():
                    # Encontrando o nome da tabela equivalente no banco do primeiro ano
                    table_name_1 = self.tables_2[table_name]
                    # Chamando o método da classe CRUD que executa a operação da linha que está na tabela de log
                    self.operations[str(operation).upper()](table_name_1, self.ids_1[table_name_1], table_name, self.ids_2[table_name], id, 2)
    
    def atualizar(self) -> None:
        self.atualizar_1()
        self.atualizar_2()