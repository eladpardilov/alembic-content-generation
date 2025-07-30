# Content Migration with Alembic

This repository contains the necessary configurations and scripts to handle content migration using Alembic.

There's a medium article that explains the process in detail. You can find it [here](link_to_it).

Below you will find instructions on setting up a local database with Docker and running Alembic migrations.

## Prerequisites

- Docker Desktop
- Python
- Alembic

## Setup

### Database Initialization

To set up the PostgreSQL database in a Docker container, run the following command:

```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password -e POSTGRES_USER=admin -e POSTGRES_DB=content-migration-db --name content-migration-db library/postgres:14.9
```

You can always stop & delete the container by running:

```bash
docker stop content-migration-db
docker rm content-migration-db
```

Afterwards, you can start the container again with an empty database by running the first command.

### Alembic Operations

In order to create a standard alembic migration (with auto generation):

```bash
# create the table/tables
alembic revision --autogenerate -m "<revision name>"
# updating the DB with the changes
alembic upgrade head
```

To create a migration that includes content changes, using the feature we've implemented in this repository,
you can run the following commands:

```bash
# using the content auto generation method to populate the table
alembic -x generate_content_changes=1 revision --autogenerate -m "<revision name>"
# updating the DB with the changes
alembic upgrade head

# after changing the table content in db_models/content/configuration_table.py,
# running the following commands will ingest the changes and create a new migration
alembic -x generate_content_changes=1 revision --autogenerate -m "<other revision name>"
# updating the DB with the changes
alembic upgrade head
```
