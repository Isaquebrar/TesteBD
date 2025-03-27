import pandas as pd
import os
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta


operadoras_file = r'C:\Users\Admin\Documentos\dicionario_de_dados_das_operadoras_ativas.csv'
eventos_file = r'C:\Users\Admin\Documentos\Relatorio_cadop.csv'


# Conectar ao MySQL
def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='testebd',
            user='root',
            password='senha'
        )
        if connection.is_connected():
            print("Conexão bem-sucedida com o MySQL")
            return connection
    except Error as e:
        print("Erro ao conectar ao MySQL:", e)
        return None


def create_tables(cursor):
    try:

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS operadoras (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nome VARCHAR(255),
                cnpj VARCHAR(20),
                codigo_operadora VARCHAR(20),
                categoria VARCHAR(100)
            );
        """)


        cursor.execute("""
            CREATE TABLE IF NOT EXISTS eventos_sinistros (
                id INT AUTO_INCREMENT PRIMARY KEY,
                operadora_id INT,
                data_evento DATE,
                despesa DECIMAL(10, 2),
                categoria VARCHAR(255),
                FOREIGN KEY (operadora_id) REFERENCES operadoras(id)
            );
        """)

        print("Tabelas criadas com sucesso!")
    except Error as e:
        print("Erro ao criar as tabelas:", e)


def import_csv_to_mysql(file_path, table_name, connection):

    if not os.path.isfile(file_path):
        print(f"Arquivo não encontrado: {file_path}")
        return

    print(f"Arquivo encontrado: {file_path}")

    try:

        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            print("Arquivo carregado 'utf-8'.")
        except UnicodeDecodeError:
            print("Erro com a codificação 'utf-8', tentando com 'ISO-8859-1'...")
            df = pd.read_csv(file_path, encoding='ISO-8859-1')
            print("Arquivo carregado 'ISO-8859-1'.")
        except Exception as e:
            print(f"Erro ao ler o arquivo CSV: {e}")
            return


        print("Primeiras linhas do arquivo CSV:")
        print(df.head())


        cursor = connection.cursor()


        for index, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO {table_name} (nome, cnpj, codigo_operadora, categoria)
                VALUES (%s, %s, %s, %s);
            """, (row['nome'], row['cnpj'], row['codigo_operadora'], row['categoria']))

        connection.commit()
        print(f"{len(df)} registros inseridos com sucesso na tabela '{table_name}'!")

    except Exception as e:
        print(f"Erro ao importar os dados para o MySQL: {e}")


def import_eventos_to_mysql(file_path, connection):

    if not os.path.isfile(file_path):
        print(f"Arquivo não encontrado: {file_path}")
        return

    try:

        df = pd.read_csv(file_path, encoding='utf-8')

        print("Arquivo de eventos carregado com sucesso.")
        print("Primeiras linhas do arquivo de eventos:")
        print(df.head())

        cursor = connection.cursor()

        for index, row in df.iterrows():

            cursor.execute("""
                INSERT INTO eventos_sinistros (operadora_id, data_evento, despesa, categoria)
                VALUES (%s, %s, %s, %s);
            """, (row['operadora_id'], row['data_evento'], row['despesa'], row['categoria']))

        connection.commit()
        print(f"{len(df)} registros de eventos inseridos com sucesso na tabela 'eventos_sinistros'!")

    except Exception as e:
        print(f"Erro ao importar os dados de eventos para o MySQL: {e}")


def analitica_ultimo_trimestre(connection):
    cursor = connection.cursor()
    data_fim = datetime.now()
    data_inicio = data_fim - timedelta(days=90)

    cursor.execute("""
        SELECT o.nome, SUM(e.despesa) as total_despesa
        FROM eventos_sinistros e
        JOIN operadoras o ON e.operadora_id = o.id
        WHERE e.data_evento BETWEEN %s AND %s
        AND e.categoria = 'EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS DE ASSISTÊNCIA A SAÚDE MEDICO HOSPITALAR'
        GROUP BY o.id
        ORDER BY total_despesa DESC
        LIMIT 10;
    """, (data_inicio.date(), data_fim.date()))

    result = cursor.fetchall()
    print("Top 10 operadoras com maiores despesas no último trimestre:")
    for row in result:
        print(row)


def analitica_ultimo_ano(connection):
    cursor = connection.cursor()
    data_fim = datetime.now()
    data_inicio = data_fim - timedelta(days=365)

    cursor.execute("""
        SELECT o.nome, SUM(e.despesa) as total_despesa
        FROM eventos_sinistros e
        JOIN operadoras o ON e.operadora_id = o.id
        WHERE e.data_evento BETWEEN %s AND %s
        AND e.categoria = 'EVENTOS/ SINISTROS CONHECIDOS OU AVISADOS DE ASSISTÊNCIA A SAÚDE MEDICO HOSPITALAR'
        GROUP BY o.id
        ORDER BY total_despesa DESC
        LIMIT 10;
    """, (data_inicio.date(), data_fim.date()))

    result = cursor.fetchall()
    print("Top 10 operadoras com maiores despesas no último ano:")
    for row in result:
        print(row)


# Main
def main():

    connection = create_connection()

    if connection:

        cursor = connection.cursor()
        create_tables(cursor)


        import_csv_to_mysql(operadoras_file, 'operadoras', connection)
        import_eventos_to_mysql(eventos_file, connection)


        analitica_ultimo_trimestre(connection)
        analitica_ultimo_ano(connection)


        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
