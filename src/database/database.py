from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

# 1. String de conexão para MariaDB (ajuste user, senha e db_name)
SQLALCHEMY_DATABASE_URL = "mariadb+pymysql://root:admin@localhost:3306/bd_recomendacao_tatica_tcc"

# 2. O Engine é quem realmente faz o trabalho de baixo nível
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# 3. SessionLocal é o que você usará no seu código para fazer consultas/inserts
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 4. A classe Base que todas as suas tabelas no models.py herdam
class Base(DeclarativeBase):
    pass