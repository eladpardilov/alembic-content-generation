import os
import sys
from logging.config import fileConfig

from alembic.script import write_hooks
from sqlalchemy import engine_from_config, pool

from alembic import context
from db_models.generate_sql_changes import (
    generate_downgrade_content_changes,
    generate_upgrade_content_changes,
)
from db_models import db_base

# pylint: disable=no-member

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None and config.attributes.get(
    "configure_logger", True
):
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = db_base.metadata

if remote_db_uri := os.getenv("DB_URL"):
    config.set_main_option("sqlalchemy.url", remote_db_uri)

should_generate_content_changes = False
if context.get_x_argument(as_dictionary=True).get(
    "generate_content_changes", ""
).lower() in ("1", "true"):
    should_generate_content_changes = True


@write_hooks.register("add_content_changes_hook")
def add_content_changes_func(filename, options):
    if not should_generate_content_changes:
        return

    lines = []
    with open(filename) as file_:
        for line in file_:
            lines.append(line)
            if line.startswith("def upgrade()") or line.startswith("def downgrade()"):
                print(f"Generating content changes for {line.split()[1]}")
                generated_section = (
                    generate_upgrade_content_changes()
                    if "upgrade" in line
                    else generate_downgrade_content_changes()
                )
                print(f"{len(generated_section)} lines generated")
                lines.extend(generated_section)

    with open(filename, "w") as to_write:
        to_write.write("".join(lines))


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
