from typing import Tuple
import os

import psycopg2
import pandas as pd
import numpy as np


def connection() -> Tuple[bool, object, object]:
    """Эта функция открывает подключение и возвращает коннекшн 
    и курсор для БД

    Returns:
        bool: флаг
        object: connect
        object: cursor
    """    
    try:
        conn = psycopg2.connect(
            host='localhost',
            port='5432',
            database='airflow',
            user='postgres',
            password='MYZv3Ietuflo'
        )
    except:
        return False, 0, 0

    cur = conn.cursor()

    return True, conn, cur


def create_db(cur: object, table_name: str, data: dict) -> int:
    """Создает таблицу если ее не существует и проверяет создание,
    либо заполненность данных.

    Args:
        cur (object): курсор БД
        table_name (str, optional): Имя таблицы которую создать. 
                                    Defaults to 'test_table'.

    Returns:
        int: флаг отработало или нет
    """
    create_column = ', '.join([column + ' ' + type 
                                for column, type in data.items()])

    cur.execute(
    f'''
    create table if not exists {table_name} ({create_column});
    '''
    )

    try:
        cur.execute(f'select * from {table_name} limit 10')
        result = cur.fetchall()
        if len(result) >= 0 and type(result) == list:
            return 1
    except:
        return 0


def count_rows(cur: object, table_name: str) -> bool:
    """Проверяет заполнена ли таблица, или они пустые

    Args:
        cur (object): курсор
        table_name (str, optional): имя таблицы. Defaults to 'test_table'.

    Returns:
        bool: False если данные есть, True если данных нет
    """
    cur.execute(f'select count(*) from {table_name} limit 10')
    result = cur.fetchone()[0]
    return False if result > 0 else True


def path_file(name: str) -> str:
    """возвращает путь до файла, путь в текушей директории

    Args:
        name (str): имя файла

    Returns:
        str: путь до файла
    """    
    return os.path.join(os.getcwd(), name)


def open_file(path: str, header: bool=True) -> object:
    """открывает и возвращает тело файла с данными

    Args:
        path (str): путь до файла
        header (bool, optional): Есть ли в файле названия колонок. 
                                 Defaults to True.

    Returns:
        object: возвращает файл
    """    
    with open(path) as file:
        csv = file.readlines()
    
    if header:
        csv = csv[1:]

    table_data = []
    for data in csv:
        data = data.replace('\n', '').split(',')
        string = f'\'{data[0]}\', {data[1]}, \'{data[2]}\', \'{data[3]}:00\', {data[4]}, \'{data[5]}\''
        table_data.append(string)
    
    return table_data


def insert_table(cur: object, data: list, 
                 table_name: str, column: dict) -> bool:
    """Вставляет данные в таблицу

    Args:
        cur (object): курсор
        data (object): данные
        table_name (str, optional): имя таблицы. Defaults to 'test_table'.

    Returns:
        bool: флаг что вставились все строки
    """
    name_column = [col for col, _ in column.items()]
    name_column = ', '.join(name_column)
    query = f"""insert into {table_name} ({name_column}) \nvalues """
    for row in data:
        query += f'\n({row}), '
    query = query[:-2] + ';'

    cur.execute(query)
    cur.execute(f'select count(1) from {table_name}')
    result_lines = cur.fetchall()[0]
    if result_lines == len(data):
        return 1
    else:
        return 0
    
    # counter = 0
    # for value in data:
    #     cur.execute(
    #         f'''insert into {table_name} ({name_column}) 
    #             values (\'{value[0]}\', {value[1]});'''
    #     )
    #     counter += 1
    
    # cur.execute(f'select * from {table_name}')
    # result_lines = len(cur.fetchall())
    # if result_lines == counter:
    #     return 1
    # else:
    #     return 0


def commit(conn: object, cur: object, 
        close: bool=False, commit: bool=False) -> None:
    """Коммит вставленных в таблицу данных

    Args:
        conn (object): соединение
        cur (object): курсор
        close (bool, optional): Флаг закрытия соединения. Defaults to False.
        commit (bool, optional): Флаг коммита. Defaults to False.
    """    
    if commit:
        conn.commit()
    if close:
        cur.close()
        conn.close()
    

def std_out(cur: object, table_name: str, data: dict) -> object:
    """Запрашивает записанные данные для проверки из БД

    Args:
        cur (object): курсор
        table_name (str, optional): имя таблицы. Defaults to 'test_table'.

    Returns:
        object: возвращает данные из таблицы
    """    
    cur.execute(f'select * from {table_name} limit 10')
    df_temp = np.array(cur.fetchall())
    column_name = [col for _, col in data.items()]
    frame = {col: df_temp[:, num] for num, col in enumerate(column_name)}
    df = pd.DataFrame({frame})
    if len(df_temp):
        return df
    else:
        print('Данные не вставились, проверьте корректность работы.')
    

def main(name: str, table: dict):
    """
    Функция выполняет весь код для записи данных в БД
    1. Создает коннект
    2. Создает таблицу если ее нет
    3. Проверяет есть ли в таблице уже данные
    4. Открывает файл
    5. Пишет файл в таблицу
    6. Выводит записанные данные
    """    
    connect_flag, conn, cur = connection()
    assert connect_flag == True, 'Нет соединения'

    create_flag = create_db(cur, table_name=name, data=table)
    assert create_flag == True, 'Таблица не создана'

    need_fill = count_rows(cur, table_name=name)
    filename = name + '.csv'
    if need_fill:
        path = path_file(filename)
        data = open_file(path)

        insert_flag = insert_table(cur, data, name, table)
        assert insert_flag == True, 'Данные не вставлены'
        print('Данные вставлены в таблицу')

        commit(conn, cur, close=False, commit=True)

    df = std_out(cur, table_name=name)
    print(df)

    commit(conn, cur, close=True, commit=False)

    
if __name__ == '__main__':
    tables = [
        {'carrier_code': 'text',
         'flight': 'int',
         'destination_airport': 'text',
         'scheduled_time': 'time',
         'delay': 'int',
         'date': 'date',}
    ]
    names = ['departures']
    for table, name in zip(tables, names):
        main(name, table)
