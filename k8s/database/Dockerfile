FROM postgis/postgis:latest

RUN sed -i -r 's/#huge_pages.*?/huge_pages = off/g' /usr/share/postgresql/postgresql.conf.sample
RUN echo 'host all all 0.0.0.0/0 md5' >> /var/lib/postgresql/data/pg_hba.conf
