import psycopg2
def connect2DB():
    return psycopg2.connect(    user = "postgres",
                                password = "password",
                                host = "127.0.0.1",
                                port = "27018",
                                database = "postgres")
