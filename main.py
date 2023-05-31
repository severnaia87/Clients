import psycopg2


def create_db(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clients(
            id SERIAL PRIMARY KEY,
            firstname VARCHAR(20),
            lastname VARCHAR(30),
            email VARCHAR(254) NOT NULL);
        """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS phonenumbers(
            id SERIAL PRIMARY KEY,    
            client_id INTEGER NOT NULL REFERENCES clients(id),
            phone VARCHAR (25) NOT NULL);
        """)
    conn.commit()


def add_client(cur, name, last_name, email, phone=None):
    if name == None or last_name == None or email == None:
        print('Имя/Фамилия/Почта/ отсутствуют')
        return

    cur.execute("""
        INSERT INTO clients(firstname, lastname, email) VALUES (%s, %s, %s) RETURNING id, firstname, 
        lastname, email;
        """, (name, last_name, email))
    new_client = cur.fetchone()
    if phone is not None:
        cur.execute("""
            INSERT INTO phones(client_id, phone) VALUES(%s, %s);
            """, (new_client[0], phone))
    cur.fetchone()
    print(f'Добавили клиента {new_client}')


def get_phone(cur, client_id, phone):
    cur.execute("""
        SELECT phone FROM phonenumbers WHERE client_id=%s AND phone=%s;
        """, (client_id, phone))
    found_phone = cur.fetchall()
    return found_phone


def add_phone(conn, cur, client_id, phone):
    found_phone = get_phone(cur, client_id, phone)
    if found_phone is None or len(found_phone) == 0:
        print(found_phone, client_id, phone)
        cur.execute("""
            INSERT INTO phonenumbers(client_id, phone) VALUES (%s, %s) RETURNING phone, id;
            """, (client_id, phone))
        conn.commit()


def change_client(conn, cur, client_id, name=None, last_name=None, email=None, phone=None):
    if name is not None:
        cur.execute("""
            UPDATE clients SET firstname=%s WHERE id=%s
            """, (name, client_id))
    if last_name is None:
        cur.execute("""
            UPDATE clients SET lastname=%s WHERE id=%s
            """, (last_name, client_id))
    if email is None:
        cur.execute("""
            UPDATE clients SET email=%s WHERE id=%s
            """, (email, client_id))
    if phone is not None:
        add_phone(conn, cur, client_id, phone)

    cur.execute("""
        SELECT * FROM clients;
        """)
    cur.fetchall()


def delete_phone(cur, client_id, phone):
    cur.execute("""
        DELETE FROM phonenumbers WHERE client_id=%s and phone=%s
        """, (client_id, phone,))
    cur.execute("""
        SELECT * FROM phonenumbers WHERE client_id=%s;
        """, (client_id))
    print(cur.fetchall())


def delete_client(cur, client_id):
    cur.execute("""
        DELETE FROM phonenumbers WHERE client_id=%s
        """, (client_id,))
    cur.execute("""
        DELETE FROM clients WHERE id=%s
        """, (client_id,))
    cur.execute("""
        SELECT * FROM clients;
        """)
    print(cur.fetchall())


def find_client(cur, name=None, last_name=None, email=None, phone=None):
    if phone is not None:
        cur.execute("""
            SELECT cl.id  FROM clients cl
            JOIN phonenumbers ph ON ph.client_id = cl.id
            WHERE ph.phone=%s
            """, (phone))
    else:
        cur.execute("""
            SELECT id FROM clients
            WHERE firstname=%s OR lastname=%s OR email=%s;
            AND c.email LIKE %s AND p.number like %s
            """, (name, last_name, email))
    print(cur.fetchall())


if __name__ == '__main__':
    with psycopg2.connect(database="Clients_DB", user="postgres", password="123456") as conn:
        with conn.cursor() as cur:
            create_db(conn, cur)

            # 1. Добавляем 5 клиентов
            add_client(cur, "Никита", "Михалков", "mihalkov@gmail.com", None)
            add_client(cur, "Дмитрий", "Маликов", "malikov@mail.ru", 79991234567)
            add_client(cur, "Стас", "Михайлов", "mixhailov@outlook.com", 79997654321)
            add_client(cur, "Ольга", "Бузова", "buzcoin@mail.ru", 79997652143)
            add_client(cur, "Карл", "Лагерфельд", "chanell@outlook.com", None)

            # 2. Добавляем клиенту номер телефона(одному первый, одному второй)
            add_phone(cur, 2, 79991236457)
            add_phone(cur, 1, 79991237654)

            # 3. Изменим данные клиента
            change_client(cur, 4, "Макар", None, 'makar@outlook.com')

            # 4. Удаляем клиенту номер телефона
            delete_phone(cur, '79991234567')

            # 5. Удалим клиента номер 2
            delete_client(cur, 2)

            # 6. Найдём клиента по определенным фильтрам
            find_client(cur, 'Стас')
            find_client(cur, None, None, 'mixhailov@outlook.com')
            find_client(cur, 'Стас', 'Михайлов', 'mixhailov@outlook.com')
            find_client(cur, 'Дмитрий', 'Маликов', 'malikov@mail.ru', '79991234567')
            find_client(cur, None, None, None, '79991234567')