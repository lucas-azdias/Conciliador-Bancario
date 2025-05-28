import colorama
import logging
import pathlib
import polars
import sqlalchemy
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
            log_path: pathlib.Path,
            can_fill: bool = True,
            can_purge: bool = False,
            has_dev_mode: bool = False
        ) -> None:
        self.__logger: logging.Logger = self.__init_logger(log_path, has_dev_mode)
        self.__engine: sqlalchemy.Engine = sqlalchemy.create_engine(database_uri)
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
            data: typing.Dict[str, typing.Any]
        ) -> typing.Tuple[int, ...]:
        if not self.has_table(table_name):
            raise Exception("Table name not found on schema tables.")

        if not data:
            raise Exception("Missing data to insert on table.")

        with self.__sessionmaker() as session:
            try:
                model: typing.Type[BaseModel.BaseModel] = self.get_model(table_name) if isinstance(table_name, str) else table_name
                instance: BaseModel.BaseModel = model(**data)
                session.add(instance)
                session.commit()

                return tuple(pk for pk in sqlalchemy.inspect(instance).identity)

            except Exception as e:
                session.rollback()
                raise Exception(f"Failed to insert record: {e}")

            finally:
                session.close()


    def extend(
            self,
            table_name: BaseModel.BaseModel | str,
            data: polars.DataFrame
        ) -> typing.Tuple[int, ...]:
        if not self.has_table(table_name):
            raise Exception("Table name not found on schema tables.")

        if data.is_empty():
            raise Exception("Missing data to extend table.")

        with self.__sessionmaker() as session:
            try:
                model: typing.Type[BaseModel.BaseModel] = self.get_model(table_name) if isinstance(table_name, str) else table_name
                instances: typing.List[BaseModel.BaseModel] = [model(**record) for record in data.to_dicts()]
                session.add_all(instances)
                session.commit()

                return tuple(pk for pk in sqlalchemy.inspect(instances[-1]).identity)

            except Exception as e:
                session.rollback()
                raise Exception(f"Failed to extend table: {e}")

            finally:
                session.close()


    def read(
            self,
            table_name: BaseModel.BaseModel | str,
            **conditions: typing.Callable[[sqlalchemy.Column], sqlalchemy.ClauseElement]
        ) -> polars.DataFrame:
        if not self.has_table(table_name):
            raise Exception("Table name not found on schema tables.")

        with self.__sessionmaker() as session:
            try:
                model: typing.Type[BaseModel.BaseModel] = self.get_model(table_name) if isinstance(table_name, str) else table_name
                query: sqlalchemy.orm.Query = session.query(model)

                if conditions:
                    for column_name, clause in conditions.items():
                        column = getattr(model, column_name, None)
                        if not column:
                            raise ValueError(f"Invalid column name \"{column_name}\" for table \"{model.__tablename__}\" was given.")
                        query = query.filter(clause(column))

                schema: typing.List[str] = [column.key for column in model.__table__.columns]
                fetched: typing.List[BaseModel.BaseModel] = query.all()

                return polars.DataFrame([instance.to_dict() for instance in fetched], schema = schema)

            except Exception as e:
                session.rollback()
                raise Exception(f"Failed to read records: {e}")

            finally:
                session.close()


    def update(
            self,
            table_name: BaseModel.BaseModel | str,
            data: typing.Dict[str, typing.Any],
            **conditions: typing.Callable[[sqlalchemy.Column], sqlalchemy.ClauseElement]
        ) -> int:
        if not self.has_table(table_name):
            raise Exception("Table name not found on schema tables.")

        if not data:
            raise Exception("Missing data to update table.")

        with self.__sessionmaker() as session:
            try:
                model: typing.Type[BaseModel.BaseModel] = self.get_model(table_name) if isinstance(table_name, str) else table_name
                query: sqlalchemy.orm.Query = session.query(model)

                if conditions:
                    for column_name, clause in conditions.items():
                        column = getattr(model, column_name, None)
                        if not column:
                            raise ValueError(f"Invalid column name \"{column_name}\" for table \"{model.__tablename__}\" was given.")
                        query = query.filter(clause(column))

                fetched: typing.List[BaseModel.BaseModel] = query.all()

                for instance in fetched:
                    for key, value in data.items():
                        setattr(instance, key, value)

                session.commit()

                return len(fetched)

            except Exception as e:
                session.rollback()
                raise Exception(f"Failed to update records: {e}")

            finally:
                session.close()


    def delete(
            self,
            table_name: BaseModel.BaseModel | str,
            **conditions: typing.Callable[[sqlalchemy.Column], sqlalchemy.ClauseElement]
        ) -> int:
        if not self.has_table(table_name):
            raise Exception("Table name not found on schema tables.")

        with self.__sessionmaker() as session:
            try:
                model: typing.Type[BaseModel.BaseModel] = self.get_model(table_name) if isinstance(table_name, str) else table_name
                query: sqlalchemy.orm.Query = session.query(model)

                if conditions:
                    for column_name, clause in conditions.items():
                        column = getattr(model, column_name, None)
                        if not column:
                            raise ValueError(f"Invalid column name \"{column_name}\" for table \"{model.__tablename__}\" was given.")
                        query = query.filter(clause(column))

                fetched: typing.List[BaseModel.BaseModel] = query.all()

                for instance in fetched:
                    session.delete(instance)

                session.commit()

                return len(fetched)

            except Exception as e:
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
        elif isinstance(table_name, str):
            return table_name in self.__inspector.get_table_names()
        raise TypeError("Invalid table name type.")


    def get_model(self, table_name: str) -> typing.Type[BaseModel.BaseModel]:
        for subcls in BaseModel.BaseModel.__subclasses__():
            if hasattr(subcls, "__tablename__") and subcls.__tablename__ == table_name:
                return subcls
        raise ValueError(f"Invalid table name \"{table_name}\" was given.")


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
            except Exception as e:
                raise Exception(f"Failed to create tables: {str(e)}")

        # Handle table purging
        tables_to_purge: typing.List[str] = [table for table in db_tables if table not in orm_tables]
        if tables_to_purge and not can_purge and should_raise_permission_errors:
            raise PermissionError(f"Not able to purge extra tables (\"purge\" permission not granted): {', '.join(tables_to_purge)}.")

        if tables_to_purge and can_purge:
            try:
                for table_name in tables_to_purge:
                    self.__db_metadata.tables[table_name].drop(self.__engine)
            except Exception as e:
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
                except Exception as e:
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
                except Exception as e:
                    raise Exception(f"Failed to purge columns in table \"{table_name}\": {str(e)}")


    def __init_logger(
            self,
            log_path: pathlib.Path,
            has_dev_mode: bool
        ) -> logging.Logger:
        logger = logging.getLogger("sqlalchemy.engine")

        # Clear existing handlers to avoid duplicates
        logger.handlers = []

        if has_dev_mode:
            class ColoredFormatter(logging.Formatter):
                def format(self, record):
                    msg = super().format(record)
                    return f"{colorama.Fore.YELLOW}{msg}{colorama.Style.RESET_ALL}"

            # Logging output to console
            logger.setLevel(logging.DEBUG)
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(ColoredFormatter(
                fmt = "%(asctime)s [%(levelname)s] %(message)s",
                datefmt = "%Y-%m-%d %H:%M:%S"
            ))
            logger.addHandler(console_handler)

        else:
            # Logging output to file
            logger.setLevel(logging.INFO)
            file_handler = logging.FileHandler(
                log_path.absolute(),
                mode = "w",
                encoding = "utf-8"
            )
            file_handler.setFormatter(logging.Formatter(
                fmt = "%(asctime)s [%(levelname)s] %(message)s",
                datefmt = "%Y-%m-%d %H:%M:%S"
            ))
            logger.addHandler(file_handler)

        return logger