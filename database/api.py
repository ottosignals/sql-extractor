import mysql.connector
from mysql.connector import FieldType

class MySQLError(Exception):
    pass

class MySQL(object):
    def __init__(self, host, database, user, password, port=3306):
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._port = port

        self._connection = self._init_connection()

    def _init_connection(self):
        print("Creating connection to DB.")
        self._connection = mysql.connector.connect(host=self._host,
                                            port=self._port,
                                            database=self._database,
                                            user=self._user,
                                            password=self._password)
        
        return self._connection
    
    def _get_cursor(self):
        try:
            self._connection.ping(
                reconnect=True,
                attempts=5,
                delay=10
            )
        except mysql.connector.InterfaceError as e:
            print(f"Reconnection to DB failed, making new connection.")
            self._init_connection()

        return self._connection.cursor(dictionary=True)
    
    def query_execute(self, query, params={}):
        print(f"Executing query: {query} with params: {params}")
        cursor = self._get_cursor()
        cursor.execute(query, params)

        result = cursor.fetchall()
        return result
    
    def get_table_schema_from_query(self, query):
        print(f"Getting table schema from query: {query}")
        cursor = self._get_cursor()
        cursor.execute(f"{query} limit 1")
        cursor.fetchall()

        result = [{"name": f"{i[0]}",
                   "type": f"{FieldType.get_info(i[1])}"} 
                  for i in cursor.description]
        print(f"Received schema: {result}")
        return result
    
    def pagination_init(self, query, page_size=10000):        
        self.pagination_end = False
        self._query_params = {"limit": page_size, "offset": 0}
        self._query = f"{query} limit %(limit)s offset %(offset)s"

    def pagination_next(self):
        if self.pagination_end:
            raise MySQLError(f"All pages have been extracted!")
    
        rows = self.query_execute(self._query, self._query_params)

        print(f'Offset: {self._query_params["offset"]}, size: {len(rows)}')
        if len(rows) < 1:
            self.pagination_end = True 

        self._query_params["offset"] += len(rows)
        return rows

