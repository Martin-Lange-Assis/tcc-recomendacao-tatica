from .database import engine, Base
from . import models  #importar o models para que o Base funcione

def create_tables():
    print("Criando tabelas no MariaDB...")
    # O metadata.create_all varre todas as classes que herdam de Base
    Base.metadata.create_all(bind=engine)
    print("Sucesso! As tabelas foram criadas.")

if __name__ == "__main__":
    create_tables()