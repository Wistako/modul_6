import sqlite3
from sqlite3 import Error

create_projects_sql = """
-- projects table
CREATE TABLE IF NOT EXISTS projects (
  id integer PRIMARY KEY,
  nazwa text NOT NULL,
  start_date text,
  end_date text
);
"""

create_tasks_sql = """
-- zadanie table
CREATE TABLE IF NOT EXISTS tasks (
  id integer PRIMARY KEY,
  project_id integer NOT NULL,
  nazwa VARCHAR(250) NOT NULL,
  opis TEXT,
  status VARCHAR(15) NOT NULL,
  start_date text NOT NULL,
  end_date text NOT NULL,
  FOREIGN KEY (project_id) REFERENCES projects (id)
);
"""

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
       with sqlite3.connect(db_file) as conn:
        print(f"Connected to {db_file}, sqlite version: {sqlite3.version}")
        return conn
    except Error as e:
       print(e)
    return conn 



def create_connection_in_memory():
   """ create a database connection to a SQLite database """
   conn = None
   try:
       conn = sqlite3.connect(":memory:")
       print(f"Connected, sqlite version: {sqlite3.version}")
   except Error as e:
       print(e)
   finally:
       if conn:
           conn.close()

def execute_sql(conn, sql):
   """ Execute sql
   :param conn: Connection object
   :param sql: a SQL script
   :return:
   """
   try:
       c = conn.cursor()
       c.execute(sql)
   except Error as e:
       print(e)

def add_project(conn, project): 
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """

    sql = '''INSERT INTO projects(nazwa, start_Date, end_date)
                VALUES(?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, project)
    conn.commit()
    return cur.lastrowid

def add_task(conn, task):
    """
    Create a new task into the task table
    :param conn:
    :param task:
    :return: task id
    """

    sql = '''INSERT INTO tasks(project_id, nazwa, opis, status, start_date, end_date)
                VALUES(?,?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
    return cur.lastrowid

def select_task_by_status(conn, status):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param status:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks WHERE status=?", (status,))

    rows = cur.fetchall()
    return rows

def select_all(conn, table):
   """
   Query all rows in the table
   :param conn: the Connection object
   :return:
   """
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()

   return rows

def select_where(conn, table, **query):
   """
   Query tasks from table with data from **query dict
   :param conn: the Connection object
   :param table: table name
   :param query: dict of attributes and values
   :return:
   """
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows

def update(conn, table, id, **kwargs):
    """
    update status, begin_date, and end date of a task
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id, )

    sql = f''' UPDATE {table}
                SET {parameters}
                WHERE id = ?'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("OK")
    except sqlite3.OperationalError as e:
        print(e)

def delete_all(conn, table):
    """
    Delete all records from table
    :param conn:
    :param table: table name
    :return:
    """
    sql = f'''DELETE FROM {table}'''
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        print(f'Deleted all reocrds from {table}')
    except sqlite3.OperationalError as e:
        print(e)

def delete_where(conn, table, **kwargs):
    """
    Delete record from table by id
    :param conn:
    :param table: table name
    :param id: record id
    """
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)

    sql = f'''DELETE FROM {table} WHERE {q}'''
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print(f'Delete from {table}, where: {q}')


if __name__ == '__main__':
    conn = create_connection(r"database.db")
    if conn is not None:
        delete_all(conn, 'tasks')
        # all = select_all(conn, 'projects')
        # print(all)
        # update(conn, "tasks", 2, status="started")
        # update(conn, "tasks", 2, status="started")
        # conn.close()
        
#         execute_sql(conn, create_projects_sql)
#         execute_sql(conn, create_tasks_sql)
       
#         project = ("Powtórka z angielskiego", "2020-05-11 00:00:00", "2020-05-13 00:00:00")
#         pr_id = add_project(conn, project)

#         task = (
#        pr_id,
#        "Czasowniki regularne",
#        "Zapamiętaj czasowniki ze strony 30",
#        "started",
#        "2020-05-11 12:00:00",
#        "2020-05-11 15:00:00"
#    )

#         task_id = add_task(conn, task)
#         conn.close() 
#         print(pr_id)
#         print(task_id)