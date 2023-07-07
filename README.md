# Desafio de Engenharia de Dados

### Tecnologias usadas
- Docker
- Apache Airflow (com Redis e Celery)
- Python
- PostgreSQL

### Para rodar o projeto em qualquer ambiente

1. Instale o Docker e o Docker-Compose na sua máquina.
2. Localize o arquivo docker-compose.yaml neste repositório ou siga o tutorial deste link (eng): <https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html>. Note que o arquivo compose desse repositório possui algumas configurações customizadas, como a exibição das configurações na UI do Airflow e o mapeamento de algumas pastas internas para pastas locais.
4. Crie um diretório para ser a raiz do projeto. Dentro desse diretório, coloque o arquivo docker-compose.yaml com a seguinte estrutura de pastas:
```
├── dags
│ └── sql
├── logs
├── plugins
├── tmp
└── docker-compose.yaml
```

4. Dentro do diretório raiz execute o comando 'docker-compose up -d' para subir a aplicação.
5. Pronto! A aplicação com o Apache Airflow estará exposta na porta 8080. Com o container sendo executado basta acessar <http://localhost:8080> com o username e password 'airflow'.


### Sobre o projeto

Foi implementada uma DAG (Directed Acyclic Graph) para realizar uma ETL (Extract, Transform and Load) de dados vindos de 3 diferentes fontes:
- API
- Banco de dados PostgreSQL
- Arquivo .parquet

Após a extração, os dados foram estruturados com dicionários para depois serem transformados em arquivos .parquet com o uso da biblioteca Pandas.
Então, os dados foram carregados em um Banco de Dados local através de scripts SQL. Após a inserção, os arquivos .parquet gerados são deletados.
A seguir, a DAG final gerada.

![image](https://github.com/itsmevicot/data_engineering_challenge/assets/78550840/69174669-9d2f-40ea-8102-6604363ff070)



