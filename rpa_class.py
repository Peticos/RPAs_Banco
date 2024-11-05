class Crud:
    # Construtor
    def __init__(self, cursor1, cursor2) -> None:
        self.cursor1 = cursor1
        self.cursor2 = cursor2
    
    # Método para passar chamar as procedures de insert correta de acordo com a tabela que precisa ter inserção:
    def insert(self, table_name_1, id_1, table_name_2, id_2, id) -> None:
        '''
        # Inserir dados no banco
        Caso algo que não esteja no banco do segundo ano tenha sido inserido no banco do primeiro, é inserido.
        
        ## Parâmetros:
        - table_name_1 - Nome da tabela do primeiro.
        - id_1 - Nome da coluna de ID da tabela do primeiro.
        - table_name_2 - Nome da tabela do segundo.
        - id_2 - Nome da coluna de ID da tabela do segundo.
        - id - id da linha inserida no banco do primeiro.
        
        '''
        # Encontrando os dados inseridos no banco do primeiro pelo ID
        self.cursor1.execute("SELECT * FROM %s WHERE %s = %s", (table_name_1, id_1, id))
        tabela = self.cursor1.fetchall()[0]

        # Caso seja inserido na tabela de ADM, os dados serão inseridos como um INSERT comum, outrossim, serão inseridos através das procedures
        if table_name_2 == 'admin':
            # Inserindo os novos dados na tabela de admin:
            self.cursor2.execute("INSERT INTO admin (id, name, email, password) VALUES(%s, %s, %s, %s)", (tabela[0], tabela[1], tabela[2], tabela[3]))
        else:
            # Inserindo os dados nas tabelas com procedures
            self.cursor2.execute(f'CALL insert_{str(table_name_2).lower()}({"%s"*len(tabela)})', tabela)
    
    # Método para chamar as procedures de update corretas para cada tabela:  
    def update(self, table_name_1, id_1, table_name_2, id_2, id):
        '''
        # Atualizar dados do banco
        Caso algo seja alterado no banco do primeiro ano, também será no do segundo.
        
        ## Parâmetros:
        - table_name_1 - Nome da tabela do primeiro.
        - id_1 - Nome da coluna de ID da tabela do primeiro.
        - table_name_2 - Nome da tabela do segundo.
        - id_2 - Nome da coluna de ID da tabela do segundo.
        - id - id da linha atualizada no banco do primeiro.
        
        '''
        
        # Encontrando os dados atualizados no banco do primeiro pelo ID
        self.cursor1.execute("SELECT * FROM admin.administradores WHERE %s = %s", (id_1, id))
        tabela = self.cursor1.fetchall()[0]

        # Caso seja atualizado na tabela de ADM, os dados serão atualizados como um UPDATE comum, outrossim, serão autualizados através das procedures
        if table_name_2 == 'admin':
            #Inserindo os novos dados na tabela de admin:
            self.cursor2.execute("UPDATE admin.administradores SET name = %s, email = %s, password = %s WHERE %s = %s", (tabela[1], tabela[2], tabela[3], id_2, id))
        elif table_name_2 == '':
            self.cursor2.execute('')

    def delete (self):
        pass
    

class RPA:
    # Construtor
    def __init__(self) -> None:
        # Importações
        import os
        from dotenv import load_dotenv
        
        # A senha do Banco de Dados vem de um .env
        load_dotenv()
        password = os.getenv('PASSWORD12')
        
        # Dicionários de tabelas e suas colunas identificadoras
        self.ids_1 = {'administradores': 'id', 'dica_do_dia': 'id_dica', 'endereco': 'id_endereco', 'local': 'id_local', 'telefone_local': 'id_telefone_local', 'raca': 'id_raca', 'especie': 'id_especie'}
        self.ids_2 = {'admin': 'id_admin', 'day_hint': 'id_hint', 'address': 'id_address', 'locations': 'id_local', 'local_phone': 'id_local_phone', 'race': 'id_race', 'specie': 'id_specie'}
        
        # Dicionário com as tabelas do primeiro ano e suas equivalentes no do segundo ano
        self.tables = {'administradores': 'admin', 'dica_do_dia': 'day_hint', 'endereco': 'address', 'local': 'locations', 'telefone_local': 'local_phone', 'raca': 'race', 'especie': 'specie'}
        
        # Instância da classe CRUD, para ter acesso aos métodos 
        self.crud = Crud(*self.connect(password))
        
        # Dicionário com as funções de cada interação
        self.operations = {'INSERT': self.crud.insert, 'UPDATE': self.crud.update, 'DELETE': self.crud.delete}
        
    
    # Método de conexão com os bancos de dados
    def connect(self, password):
        '''
        # Conecta com os dois bancos e retorna os cursores
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
    
    def atualizar_2(self):
        self.cursor1.execute("SELECT * FROM log_total_1 WHERE alteration_date >= CURRENT_TIMESTAMP - INTERVAL '5 minutes';")
        modifications = self.cursor1.fetchall()
            
        if modifications != []:
            for modification in modifications:
                alteration_date, id, table_name, operation, user = modification
                table_name_2 = self.tables[table_name]
                self.operations[str(operation).upper()](table_name, self.ids_1[table_name], table_name_2, self.ids_2[table_name_2], id)
                
    def atualizar_1(self):
        pass