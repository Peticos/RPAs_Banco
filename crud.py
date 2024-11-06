class Crud:
    # Construtor
    def __init__(self, cursor1, cursor2) -> None:
        #Importações
        import psycopg2 as pg
        
        self.cursor1 = cursor1
        self.cursor2 = cursor2
    
    # Método para passar chamar as procedures de insert correta de acordo com a tabela que precisa ter inserção:
    def insert(self, table_name_1: str, id_1: str, table_name_2:str, id_2: str, id: int, db: int) -> None:
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
        
        if db == 1:
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
        else:
            # Encontrando os dados inseridos no banco do segundo pelo ID
            string = f'select_{str(table_name_2.lower())}'
            self.cursor2.execute("SELECT * FROM"+string+"(%s)", id)
            tabela = self.cursor2.fetchall()[0]
            
            self.cursor1.execute(f'CALL insert_{str(table_name_1).lower()}({"%s"*len(tabela)})', tabela)
    
    # Método para chamar as procedures de update corretas para cada tabela:  
    def update(self, table_name_1: str, id_1: str, table_name_2: str, id_2: str, id: int, db: int) -> None:
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
        
        if db == 1:
            # Encontrando os dados atualizados no banco do primeiro pelo ID
            self.cursor1.execute("SELECT * FROM admin.administradores WHERE %s = %s", (id_1, id))
            tabela = self.cursor1.fetchall()[0]

            # Caso seja atualizado na tabela de ADM, os dados serão atualizados como um UPDATE comum, outrossim, serão autualizados através das procedures
            if table_name_2 == 'admin':
                #Inserindo os novos dados na tabela de admin:
                self.cursor2.execute("UPDATE admin.administradores SET name = %s, email = %s, password = %s WHERE %s = %s", (tabela[1], tabela[2], tabela[3], id_2, id))
            else:
                self.cursor2.execute(f'CALL update_{str(table_name_2).lower()}({"%s"*len(tabela)})', tabela)
        else:
            # Encontrando os dados atualizados no banco do segundo pelo ID
            self.cursor2.execute("SELECT * FROM rpa_select_"+str(table_name_2.lower())+"(%s)", id)
            tabela = self.cursor2.fetchall()[0]
            
            self.cursor1.execute(f'CALL update_{str(table_name_1).lower()}({"%s"*len(tabela)})', tabela)


    def delete (self, table_name_1: str, id_1: str, table_name_2: str, id_2: str, id: int, db: int) -> None:
        '''
        # Apagar dados do banco
        Caso algo seja removido do banco do primeiro ano, também será no do segundo.
        
        ## Parâmetros:
        - table_name_1 - Nome da tabela do primeiro.
        - id_1 - Nome da coluna de ID da tabela do primeiro.
        - table_name_2 - Nome da tabela do segundo.
        - id_2 - Nome da coluna de ID da tabela do segundo.
        - id - id da linha atualizada no banco do primeiro.
        - bd - 
        
        '''
        
        if db == 1:
           self.cursor2.execute('DELETE FROM %s WHERE'+id_2+' = %s', (table_name_2, id))
        else:        
            self.cursor1.execute('DELETE FROM %s WHERE'+id_1+' = %s', (table_name_1, id))
