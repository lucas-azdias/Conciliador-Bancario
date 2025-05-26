import polars
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.orm
import typeguard
import typing

from . import BaseModel
from .models import * # Build all ORM models into Base.metadata


@typeguard.typechecked
class Database():

    def __init__(
            self,
            database_uri: str,
            can_fill: bool = True,
            can_purge: bool = False,
            has_dev_mode: bool = False
        ) -> None:
        self.__engine: sqlalchemy.Engine = sqlalchemy.create_engine(database_uri, echo = has_dev_mode)
        self.__db_metadata: sqlalchemy.MetaData = sqlalchemy.MetaData()
        self.__orm_metadata: sqlalchemy.MetaData = BaseModel.BaseModel.metadata
        self.__sessionmaker = sqlalchemy.orm.sessionmaker(bind = self.__engine)
        self.__inspector: sqlalchemy.Inspector = sqlalchemy.inspect(self.__engine)

        # Load schema from database
        self.__db_metadata.reflect(bind = self.__engine)

        # Validate schema from database and schema defined via ORM
        self.sync_schema(can_fill = can_fill, can_purge = can_purge, should_raise_permission_errors = True)

        # Update elements after sync
        self.__inspector: sqlalchemy.Inspector = sqlalchemy.inspect(self.__engine)
        self.__db_metadata.reflect(bind = self.__engine)


    def insert(
            self,
            table_name: BaseModel.BaseModel | str,
            **data: typing.Dict[str, typing.Any]
        ) -> int:
        if not self.has_table(table_name):
            raise Exception("Table name not found on schema tables.")

        with self.__sessionmaker() as session:
            try:
                table: sqlalchemy.Table = self.__db_metadata.tables.get(
                    table_name.__tablename__ if isinstance(BaseModel.BaseModel) else table_name
                )
                query: sqlalchemy.Insert = sqlalchemy.insert(table).values(**data)
                result: sqlalchemy.CursorResult = session.execute(query)
                session.commit()

                return result.inserted_primary_key[0] if result.inserted_primary_key else -1

            except sqlalchemy.exc.SQLAlchemyError as e:
                session.rollback()
                raise Exception(f"Failed to insert record: {e}")

            finally:
                session.close()


    def extend(
            self,
            table_name: BaseModel.BaseModel | str,
            data: polars.DataFrame
        ) -> typing.Tuple[int, int]:
        if not self.has_table(table_name):
            raise Exception("Table name not found on schema tables.")

        with self.__sessionmaker() as session:
            try:
                rows_affected: int = data.write_database(
                    table_name.__tablename__ if isinstance(BaseModel.BaseModel) else table_name,
                    self.__engine,
                    if_table_exists = "append",
                    engine = "sqlalchemy"
                )

                # Get last inserted id
                table: sqlalchemy.Table = self.__db_metadata.tables.get(
                    table_name.__tablename__ if isinstance(BaseModel.BaseModel) else table_name
                )
                pk_col: sqlalchemy.Column = list(table.primary_key.columns)[0]
                query: sqlalchemy.Select = sqlalchemy.select(sqlalchemy.func.max(pk_col))
                last_id: int = session.execute(query).scalar_one_or_none()

                return (rows_affected, last_id if last_id else -1)

            except sqlalchemy.exc.SQLAlchemyError as e:
                session.rollback()
                raise Exception(f"Failed to extend table: {e}")

            finally:
                session.close()


    def read(
            self,
            table_name: BaseModel.BaseModel | str,
            **conditions: typing.Optional[typing.Dict[str, typing.Any]]
        ) -> polars.DataFrame:
        if not self.has_table(table_name):
            raise Exception("Table name not found on schema tables.")

        with self.__sessionmaker() as session:
            try:
                table: sqlalchemy.Table = self.__db_metadata.tables.get(
                    table_name.__tablename__ if isinstance(BaseModel.BaseModel) else table_name
                )
                query: sqlalchemy.Select = sqlalchemy.select(table)

                if conditions:
                    query.where(
                        sqlalchemy.and_(
                            *[table.c[k] == v for k, v in conditions.items()]
                        )
                    )

                fetched: polars.DataFrame = polars.read_database(
                    query = str(query.compile(self.__engine, compile_kwargs = {"literal_binds": True})),
                    connection = self.__engine
                )

                return fetched

            except sqlalchemy.exc.SQLAlchemyError as e:
                session.rollback()
                raise Exception(f"Failed to read records: {e}")

            finally:
                session.close()


    def update(
            self,
            table_name: BaseModel.BaseModel | str,
            data: typing.Dict[str, typing.Any],
            **conditions: typing.Dict[str, typing.Any]
        ) -> int:
        if not self.has_table(table_name):
            raise Exception("Table name not found on schema tables.")

        with self.__sessionmaker() as session:
            try:
                table: sqlalchemy.Table = self.__db_metadata.tables.get(
                    table_name.__tablename__ if isinstance(BaseModel.BaseModel) else table_name
                )
                query: sqlalchemy.Update = (
                    sqlalchemy.update(table)
                    .where(sqlalchemy.and_(*[table.c[k] == v for k, v in conditions.items()]))
                    .values(**data)
                )
                result: sqlalchemy.CursorResult = session.execute(query)
                session.commit()

                return result.rowcount

            except sqlalchemy.exc.SQLAlchemyError as e:
                session.rollback()
                raise Exception(f"Failed to update records: {e}")

            finally:
                session.close()


    def delete(
            self,
            table_name: BaseModel.BaseModel | str,
            **conditions: typing.Dict[str, typing.Any]
        ) -> int:
        if not self.has_table(table_name):
            raise Exception("Table name not found on schema tables.")

        with self.__sessionmaker() as session:
            try:
                table: sqlalchemy.Table = self.__db_metadata.tables.get(
                    table_name.__tablename__ if isinstance(BaseModel.BaseModel) else table_name
                )
                query: sqlalchemy.Delete = (
                    sqlalchemy.delete(table)
                    .where(sqlalchemy.and_(*[table.c[k] == v for k, v in conditions.items()]))
                )
                result: sqlalchemy.CursorResult = session.execute(query)
                session.commit()

                return result.rowcount

            except sqlalchemy.exc.SQLAlchemyError as e:
                session.rollback()
                raise Exception(f"Failed to delete records: {e}")

            finally:
                session.close()


    def has_table(
            self,
            table_name: BaseModel.BaseModel | str
        ) -> bool:
        if isinstance(table_name, BaseModel.BaseModel):
            return table_name.__tablename__ in self.__inspector.get_table_names()
        if isinstance(table_name, str):
            return table_name in self.__inspector.get_table_names()
        raise TypeError("Invalid table name type.")


    def sync_schema(
            self,
            can_fill: bool = False,
            can_purge: bool = False,
            should_raise_permission_errors: bool = False
        ) -> None:
        db_tables: typing.List[str] = self.__inspector.get_table_names()
        orm_tables: typing.List[str] = list(self.__orm_metadata.tables.keys())

        # Handle table creation
        tables_to_create: typing.List[str] = [table for table in orm_tables if table not in db_tables]
        if tables_to_create and not can_fill and should_raise_permission_errors:
            raise PermissionError(f"Not able to create missing tables (\"fill\" permission not granted): {', '.join(tables_to_create)}.")

        if tables_to_create and can_fill:
            try:
                for table_name in tables_to_create:
                    self.__orm_metadata.tables[table_name].create(self.__engine)
            except sqlalchemy.exc.SQLAlchemyError as e:
                raise Exception(f"Failed to create tables: {str(e)}")

        # Handle table purging
        tables_to_purge: typing.List[str] = [table for table in db_tables if table not in orm_tables]
        if tables_to_purge and not can_purge and should_raise_permission_errors:
            raise PermissionError(f"Not able to purge extra tables (\"purge\" permission not granted): {', '.join(tables_to_purge)}.")

        if tables_to_purge and can_purge:
            try:
                for table_name in tables_to_purge:
                    self.__db_metadata.tables[table_name].drop(self.__engine)
            except sqlalchemy.exc.SQLAlchemyError as e:
                raise Exception(f"Failed to purge tables: {str(e)}")

        # Handle column synchronization for common tables
        common_tables: typing.List[str] = [table for table in db_tables if table in orm_tables]
        for table_name in common_tables:
            db_columns: typing.List[str] = [col["name"] for col in self.__inspector.get_columns(table_name)]
            orm_columns: typing.List[str] = list(self.__orm_metadata.tables[table_name].c.keys())

            # Handle column creation
            columns_to_create: typing.List[str] = [col for col in orm_columns if col not in db_columns]
            if columns_to_create and not can_fill and should_raise_permission_errors:
                raise PermissionError(f"Not able to create missing columns in table \"{table_name}\" (\"fill\" permission not granted): {', '.join(columns_to_create)}.")

            if columns_to_create and can_fill:
                try:
                    for column_name in columns_to_create:
                        column: sqlalchemy.Column = self.__orm_metadata.tables[table_name].c[column_name]
                        with self.__engine.connect() as conn:
                            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column.type};")
                except sqlalchemy.exc.SQLAlchemyError as e:
                    raise Exception(f"Failed to create columns in table \"{table_name}\": {str(e)}")

            # Handle column purging
            columns_to_purge: typing.List[str] = [col for col in db_columns if col not in orm_columns]
            if columns_to_purge and not can_purge and should_raise_permission_errors:
                raise PermissionError(f"Not able to purge extra columns in table \"{table_name}\" (\"purge\" permission not granted): {', '.join(columns_to_purge)}.")

            if columns_to_purge and can_purge:
                try:
                    for column_name in columns_to_purge:
                        with self.__engine.connect() as conn:
                            conn.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name};")
                except sqlalchemy.exc.SQLAlchemyError as e:
                    raise Exception(f"Failed to purge columns in table \"{table_name}\": {str(e)}")