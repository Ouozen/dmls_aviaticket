import psycopg2


def connection():
    """Эта функция открывает подключение и возвращает коннекшн 
    и курсор для БД

    Returns:
        bool: флаг
        object: connect
        object: cursor
    """    
    try:
        conn = psycopg2.connect(
            host='postgres',
            port='5432',
            database='airflow',
            user='postgres',
            password='MYZv3Ietuflo'
        )
    except:
        return False, 0, 0

    cur = conn.cursor()

    return True, conn, cur


# создать подлкючение
#_, conn, cur = connection()


#cur.execute(
#    f'''
#    select * from table;
#    '''
#    )
#data = cur.fetchall() # получить данные
#data = cur.fetchone() # используется реже, возвращает только первую строку

"""
коммит скорее всего не понадобится. При селекте не используется, скорее при insert
"""
#conn.commit()


"""
если при запросе выкидывает aborted то значит операция заблочена
обычно так происходит если был неверный запрос по синтаксису или 
бывает постгрес что-то показалось и он кинул блок
Тогда помогает ролбек, но при этом отменятся все примененные изменения,
в случае с селектом не страшно, страшно если что-то инсертил или удалял.
"""
#conn.rollback()