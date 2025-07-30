import os
from pathlib import Path

import pytest
from alembic.config import Config
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

from alembic import command
from db_models import db_base
from db_models.content.configuration_table import table_content as configurations_table_content
from db_models.tables.configuration_table import ConfigurationsTable


TEST_DB_FILENAME = "sqlite3_test_db"
ALEMBIC_INI = "alembic.ini"


def _get_content_from_orm_object(obj, skip_columns: list = None):
    return tuple(
        getattr(obj, col.name)
        for col in obj.__table__.columns
        if col.name not in skip_columns
    )


@pytest.fixture(scope="session")
def post_alembic_engine():
    # Use file-based local DB for testing, if file exists, remove it
    if os.path.exists(TEST_DB_FILENAME):
        os.remove(TEST_DB_FILENAME)

    engine = create_engine(f"sqlite:///{TEST_DB_FILENAME}")

    # Config alembic
    alembic_cfg = Config(
        ALEMBIC_INI, attributes={"configure_logger": False}
    )
    alembic_cfg.set_main_option("sqlalchemy.url", str(engine.url))

    # Run migrations
    command.upgrade(alembic_cfg, "head")

    yield engine

    # Clean up the database after the tests
    engine.dispose()
    os.remove(TEST_DB_FILENAME)


@pytest.fixture
def session(post_alembic_engine):
    return sessionmaker(bind=post_alembic_engine)()


@pytest.fixture
def inspector(post_alembic_engine):
    return inspect(post_alembic_engine)


def test_migration_table_integrity(inspector):
    # compare the tables schema in the repo with the tables in the remote db - this is a sanity check
    remote_tables = {}
    repo_tables = {}

    for k, v in db_base.metadata.tables.items():
        repo_tables[k] = set([col.name for col in v.columns])
        remote_tables[k] = set([col["name"] for col in inspector.get_columns(k)])

    assert repo_tables == remote_tables


def test_models_table_content(session):
    # compare the configurations table content with the expected content
    expected_content = [
        _get_content_from_orm_object(elem, skip_columns=[ConfigurationsTable.updated_at.key])
        for elem in configurations_table_content
    ]
    actual_content = session.query(
        ConfigurationsTable.service,
        ConfigurationsTable.version,
        ConfigurationsTable.hostname,
        ConfigurationsTable.port,
        ConfigurationsTable.status,
    ).all()

    assert set(expected_content) == set(actual_content)


def test_models_table_constraints(session):
    # validate that only one row holds status "on" for each service
    distinct_on_versions = (
        session.query(ConfigurationsTable.service, ConfigurationsTable.version)
        .filter(ConfigurationsTable.status == "on")
        .group_by(ConfigurationsTable.service)
        .all()
    )
    all_on_versions = (
        session.query(ConfigurationsTable.service, ConfigurationsTable.version)
        .filter(ConfigurationsTable.status == "on")
        .all()
    )

    assert set(all_on_versions) == set(distinct_on_versions)
