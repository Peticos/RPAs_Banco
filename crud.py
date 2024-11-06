class Crud:
    # Construtor
    def __init__(self, cursor1, cursor2) -> None:
        # Importações
        import psycopg2 as pg

        self.cursor1 = cursor1
        self.cursor2 = cursor2
        print("Crud class instantiated successfully with provided cursors.")

    # Método para inserir dados no banco
    def insert(self, table_name_1: str, id_1: str, table_name_2: str, id_2: str, id: int, db: int) -> None:
        '''
        Inserir dados no banco.
        Se algo for inserido no banco do primeiro ano, será inserido também no do segundo, e vice-versa.
        '''
        print(f"Insert method called with parameters: {table_name_1}, {id_1}, {table_name_2}, {id_2}, {id}, {db}")
        try:
            if db == 1:
                print(f"Fetching data from {table_name_1} where {id_1} = {id}")
                self.cursor1.execute(f"SELECT * FROM {table_name_1} WHERE {id_1} = %s", (id,))
                tabela = self.cursor1.fetchone()
                if tabela is None:
                    print(f"No data found for {table_name_1} where {id_1} = {id}")
                    return
                self.cursor1.connection.commit()

                print(f"Data retrieved: {tabela}")

                # Inserir na tabela correspondente no banco 2
                if table_name_2 == 'admin':
                    print(f"Inserting data into {table_name_2} table")
                    self.cursor2.execute("INSERT INTO admin (id_admin, name, email, password) VALUES (%s, %s, %s, %s)", 
                                          (tabela[0], tabela[1], tabela[2], tabela[3]))
                else:
                    placeholders = ', '.join(['%s'] * len(tabela))
                    print(f"Calling stored procedure: insert_{table_name_2.lower()}")
                    self.cursor2.execute(f'CALL insert_{table_name_2.lower()}({placeholders})', tabela)
                    self.cursor2.connection.commit()

            else:
                print(f"Fetching data from second DB for {table_name_2} where {id_2} = {id}")
                self.cursor2.execute(f"SELECT * FROM rpa_select_{table_name_2.lower()}(%s::INT)", (id,))
                tabela = self.cursor2.fetchone()
                if tabela is None:
                    print(f"No data found for {table_name_2} where {id_2} = {id}")
                    return
                self.cursor2.connection.commit()

                print(f"Data retrieved: {tabela}")

                placeholders = ', '.join(['%s'] * len(tabela))
                print(f"Inserting data into {table_name_1} table")
                self.cursor1.execute(f'CALL insert_{table_name_1.lower()}({placeholders})', tabela)
                self.cursor1.connection.commit()

            print("Insert operation completed successfully!")

        except Exception as e:
            print(f"Error occurred during insert operation: {e}")

    # Método para atualizar dados no banco
    def update(self, table_name_1: str, id_1: str, table_name_2: str, id_2: str, id: int, db: int) -> None:
        '''
        Atualizar dados no banco.
        Caso algo seja alterado no banco do primeiro ano, também será no do segundo.
        '''
        try:
            if db == 1:
                print(f"Fetching data from {table_name_1} where {id_1} = {id}")
                self.cursor1.execute(f"SELECT * FROM {table_name_1} WHERE {id_1} = %s", (id,))
                tabela = self.cursor1.fetchone()
                if tabela is None:
                    print(f"No data found for {table_name_1} where {id_1} = {id}")
                    return
                self.cursor1.connection.commit()

                print(f"Data retrieved: {tabela}")

                if table_name_2 == 'admin':
                    print(f"Updating data in {table_name_2} table")
                    self.cursor2.execute("UPDATE admin SET name = %s, email = %s, password = %s WHERE id = %s", 
                                          (tabela[1], tabela[2], tabela[3], tabela[0]))
                else:
                    placeholders = ', '.join(['%s'] * len(tabela))
                    print(f"Calling stored procedure: update_{table_name_2.lower()}")
                    self.cursor2.execute(f'CALL update_{table_name_2.lower()}({placeholders})', tabela)

            else:
                print(f"Fetching data from second DB for {table_name_2} where {id_2} = {id}")
                self.cursor2.execute(f"SELECT * FROM {table_name_2} WHERE {id_2} = %s", (id,))
                tabela = self.cursor2.fetchone()
                if tabela is None:
                    print(f"No data found for {table_name_2} where {id_2} = {id}")
                    return
                self.cursor2.connection.commit()

                print(f"Data retrieved: {tabela}")

                placeholders = ', '.join(['%s'] * len(tabela))
                print(f"Updating data in {table_name_1} table")
                self.cursor1.execute(f'CALL update_{table_name_1.lower()}({placeholders})', tabela)

            print("Update operation completed successfully!")

        except Exception as e:
            print(f"Error occurred during update operation: {e}")

    # Método para deletar dados do banco
    def delete(self, table_name_1: str, id_1: str, table_name_2: str, id_2: str, id: int, db: int) -> None:
        '''
        Apagar dados do banco.
        Caso algo seja removido do banco do primeiro ano, também será no do segundo.
        '''
        try:
            if db == 1:
                print(f"Deleting data from {table_name_2} where {id_2} = {id}")
                self.cursor2.execute(f"DELETE FROM {table_name_2} WHERE {id_2} = %s", (id,))
                self.cursor2.connection.commit()
            else:
                print(f"Deleting data from {table_name_1} where {id_1} = {id}")
                self.cursor1.execute(f"DELETE FROM {table_name_1} WHERE {id_1} = %s", (id,))
                self.cursor1.connection.commit()

            print("Delete operation completed successfully!")

        except Exception as e:
            print(f"Error occurred during delete operation: {e}")
