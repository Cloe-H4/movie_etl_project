from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://postgres:########@localhost:5432/movielens_db")

try:
    connection = engine.connect()
    query = connection.execute(text("SELECT 1"))
    print("Database is reachable:", query.fetchone())
    connection.close()
except Exception as error:
    print("Error:", error)