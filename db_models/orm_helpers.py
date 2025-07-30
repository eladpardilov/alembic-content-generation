import sqlalchemy as sa
from sqlalchemy import Table
from sqlalchemy.sql.expression import Update, Insert, Delete
from sqlalchemy.sql.elements import BooleanClauseList

from db_models import db_base

LOCAL_SUFFIX = "_local"
REMOTE_SUFFIX = "_remote"


def _get_primary_key_cols(table):
    return [col.name for col in table.__table__.primary_key]


def _get_primary_key_values(table, row, as_dict=True):
    if as_dict:
        return {col: row[col] for col in _get_primary_key_cols(table)}
    return [row[col] for col in _get_primary_key_cols(table)]


def _get_automation_managed_cols(table):
    return [
        col.name
        for col in table.__table__.columns
        if "exclude_from_automation" not in col.info
    ]


def _get_automation_managed_values(table, row, as_dict=True):
    if as_dict:
        return {col: row[col] for col in _get_automation_managed_cols(table)}
    return [row[col] for col in _get_automation_managed_cols(table)]


def _get_non_empty_pairs(diff_dict):
    return {k: v for k, v in diff_dict.items() if not v.empty}


def _get_compared_cols(primary_key_cols, automation_managed_cols):
    return [col for col in automation_managed_cols if col not in primary_key_cols]


def _get_compared_values(table, row, as_dict=True, upgrade_from_local=True):
    if as_dict:
        return {
            col: row[col + (LOCAL_SUFFIX if upgrade_from_local else REMOTE_SUFFIX)]
            for col in _get_compared_cols(
                _get_primary_key_cols(table), _get_automation_managed_cols(table)
            )
        }
    return [
        row[col + (LOCAL_SUFFIX if upgrade_from_local else REMOTE_SUFFIX)]
        for col in _get_compared_cols(
            _get_primary_key_cols(table), _get_automation_managed_cols(table)
        )
    ]


def _generate_update_orm_query(table_obj: Table, orm_stmt: BooleanClauseList, update_dict: dict) -> Update:
    return table_obj.update().where(orm_stmt).values(**update_dict)


def generate_update_query(
    table_name: str, primary_keys: dict, update_dict: dict
) -> Update:
    table_obj = db_base.metadata.tables[table_name]
    return _generate_update_orm_query(
        table_obj,
        sa.and_(*(table_obj.c[key] == value for key, value in primary_keys.items())),
        update_dict,
    )


def _generate_insert_orm_query(table_obj: Table, insert_dicts: list) -> Insert:
    for row_dict in insert_dicts:
        # validate that row_dict follows the schema of the table
        table_class = next(
            (
                cls.entity
                for cls in db_base.registry.mappers
                if cls.mapped_table is table_obj
            ),
            None,
        )
        if table_class:
            table_class(**row_dict)
        else:
            raise ValueError(
                f"Table {table_obj.__tablename__} is not mapped to any class in the ORM"
            )
    return table_obj.insert().values(insert_dicts)


def generate_insert_query(table_name: str, insert_dicts: list) -> Insert:
    table_obj = db_base.metadata.tables[table_name]
    return _generate_insert_orm_query(table_obj, insert_dicts)


def _generate_delete_orm_query(table_obj: Table, orm_stmt: BooleanClauseList) -> Delete:
    return table_obj.delete().where(orm_stmt)


def generate_delete_query(table_name: str, primary_keys: dict) -> Delete:
    table_obj = db_base.metadata.tables[table_name]
    return _generate_delete_orm_query(
        table_obj,
        sa.and_(*(table_obj.c[key] == value for key, value in primary_keys.items())),
    )
