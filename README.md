# calcite-examples
Examples and experimentation around Apache Calcite


Setup for Postgres/SQLite3 example (from https://ryaneschinger.com/blog/dockerized-postgresql-development-environment/)

sudo docker pull postgres/postgres:10.5
sudo docker create -v /var/lib/postgresql/data --name postgres10.5-data busybox # we decouple volume from DB image
sudo docker run -it --link local-postgres10.5:postgres --rm postgres:10.5 sh -c 'exec psql -h "$POSTGRES_PORT_5432_TCP_ADDR" -p "$POSTGRES_PORT_5432_TCP_PORT" -U postgres'
sudo docker run --name local-postgres10.5 -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d --volumes-from postgres10.5-data postgres:10.5

sudo docker rm -v local-postgres10.5
