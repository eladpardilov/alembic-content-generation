import os
from functools import lru_cache

import pandas as pd
from sqlalchemy import create_engine

from db_models.orm_helpers import (
    LOCAL_SUFFIX,
    REMOTE_SUFFIX,
    _get_automation_managed_cols,
    _get_automation_managed_values,
    _get_compared_cols,
    _get_compared_values,
    _get_non_empty_pairs,
    _get_primary_key_cols,
    _get_primary_key_values,
)
from db_models.monitored_tables import MONITORED_TABLES

DEFAULT_DB_URI = "postgresql://admin:password@localhost:5432/content-migration-db"


def find_deleted_rows(
    local_content, remote_content, primary_key_cols, automation_managed_cols
):
    # Find deleted rows (present in remote_content but not in local_content)
    deleted = pd.merge(
        remote_content,
        local_content,
        on=primary_key_cols,
        how="left",
        suffixes=("", LOCAL_SUFFIX),
        indicator=True,
    )
    deleted = deleted[deleted["_merge"] == "left_only"].drop(columns="_merge")
    return deleted[automation_managed_cols]


def find_added_rows(
    local_content, remote_content, primary_key_cols, automation_managed_cols
):
    # Find added rows (present in local_content but not in remote_content)
    added = pd.merge(
        local_content,
        remote_content,
        on=primary_key_cols,
        how="left",
        suffixes=("", REMOTE_SUFFIX),
        indicator=True,
    )
    added = added[added["_merge"] == "left_only"].drop(columns="_merge")
    return added[automation_managed_cols]


def find_updated_rows(
    local_content, remote_content, primary_key_cols, automation_managed_cols
):
    # Find updated rows (present in both dataframes but with different values)
    updated = pd.merge(
        remote_content,
        local_content,
        on=primary_key_cols,
        suffixes=(REMOTE_SUFFIX, LOCAL_SUFFIX),
    )
    updated = updated[~updated[primary_key_cols].isna().any(axis=1)]
    compared_cols = _get_compared_cols(primary_key_cols, automation_managed_cols)
    compared_remote_cols = [col + REMOTE_SUFFIX for col in compared_cols]
    compared_local_cols = [col + LOCAL_SUFFIX for col in compared_cols]

    result = pd.DataFrame(columns=updated.columns)
    for remote_col, local_col in zip(compared_remote_cols, compared_local_cols):
        result = result.append(updated[updated[remote_col] != updated[local_col]])
    return result[primary_key_cols + compared_remote_cols + compared_local_cols].drop_duplicates()


def generate_upgrade_content_changes() -> list[str]:
    deleted, added, updated = _get_differences()
    lines = ["    # upgrade content changes\n"]
    for table, entries in _get_non_empty_pairs(deleted).items():
        lines.append(f"    # deleted rows from {table.__tablename__}\n")
        for _, row in entries.iterrows():
            lines.append(
                "    op.execute(orm_helpers.generate_delete_query('{}', {}))\n".format(
                    table.__tablename__, _get_primary_key_values(table, row)
                )
            )
    for table, entries in _get_non_empty_pairs(added).items():
        lines.append(f"    # added rows from {table.__tablename__}\n")
        for _, row in entries.iterrows():
            lines.append(
                "    op.execute(orm_helpers.generate_insert_query('{}', [{}]))\n".format(
                    table.__tablename__, _get_automation_managed_values(table, row)
                )
            )
    for table, entries in _get_non_empty_pairs(updated).items():
        lines.append(f"    # updated rows from {table.__tablename__}\n")
        for _, row in entries.iterrows():
            lines.append(
                "    op.execute(orm_helpers.generate_update_query('{}', {}, {}))\n".format(
                    table.__tablename__,
                    _get_primary_key_values(table, row),
                    _get_compared_values(table, row),
                )
            )
    return lines


def generate_downgrade_content_changes() -> list[str]:
    # change downgrade the same way as upgrade
    deleted, added, updated = _get_differences()
    lines = ["    # downgrade content changes\n"]

    for table, entries in _get_non_empty_pairs(deleted).items():
        lines.append(f"    # revert deleted rows from {table.__tablename__}\n")
        for _, row in entries.iterrows():
            lines.append(
                "    op.execute(orm_helpers.generate_insert_query('{}', [{}]))\n".format(
                    table.__tablename__, _get_automation_managed_values(table, row)
                )
            )
    for table, entries in _get_non_empty_pairs(added).items():
        lines.append(f"    # revert added rows from {table.__tablename__}\n")
        for _, row in entries.iterrows():
            lines.append(
                "    op.execute(orm_helpers.generate_delete_query('{}', {}))\n".format(
                    table.__tablename__, _get_primary_key_values(table, row)
                )
            )
    for table, entries in _get_non_empty_pairs(updated).items():
        lines.append(f"    # revert updated rows from {table.__tablename__}\n")
        for _, row in entries.iterrows():
            lines.append(
                "    op.execute(orm_helpers.generate_update_query('{}', {}, {}))\n".format(
                    table.__tablename__,
                    _get_primary_key_values(table, row),
                    _get_compared_values(table, row, upgrade_from_local=False),
                )
            )

    return lines


@lru_cache(maxsize=1)
def _get_differences():
    deleted, added, updated = {}, {}, {}

    engine = create_engine(os.getenv("DB_URI", DEFAULT_DB_URI))
    # read content tables from DB into ORM list
    for table, content in MONITORED_TABLES.items():
        local_content = pd.DataFrame(
            [row.__dict__ for row in content], columns=table.__table__.c.keys()
        )

        # loose, relying on column order
        with engine.connect() as conn:
            query = conn.execute(table.__table__.select())
            remote_content_orm = [list(row) for row in query.fetchall()]

        remote_content = pd.DataFrame(
            remote_content_orm, columns=table.__table__.c.keys()
        )

        primary_key_cols = _get_primary_key_cols(table)
        automation_managed_cols = _get_automation_managed_cols(table)
        deleted[table] = find_deleted_rows(
            local_content, remote_content, primary_key_cols, automation_managed_cols
        )
        added[table] = find_added_rows(
            local_content, remote_content, primary_key_cols, automation_managed_cols
        )
        updated[table] = find_updated_rows(
            local_content, remote_content, primary_key_cols, automation_managed_cols
        )

    return deleted, added, updated
