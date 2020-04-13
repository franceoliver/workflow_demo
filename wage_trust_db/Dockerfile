FROM postgres:12
COPY /sql/db_setup.sql /docker-entrypoint-initdb.d/1-create-tables.sql
RUN chown -R postgres:postgres /docker-entrypoint-initdb.d
