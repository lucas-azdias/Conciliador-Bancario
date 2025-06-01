import datetime
import holidays
import math
import pathlib
import re
import holidays.countries
import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from ..loaders import InsertionsLoader
from . import BaseModel
from .models import Finisher, FinisherPattern, Rate, Report, StatementEntry, StatementEntryPattern, Type


@typeguard.typechecked
class ModelsConfig():

    @staticmethod
    def setup_models(
            session: sqlalchemy.orm.Session,
            insertions_path: typing.Optional[pathlib.Path] = None
        ) -> None:
        loader: InsertionsLoader.InsertionsLoader = InsertionsLoader.InsertionsLoader()

        # Insert all instances not already in database
        for table_name, dataframe in loader.process_file(insertions_path).items():
            model: typing.Type[BaseModel.BaseModel] = BaseModel.BaseModel.get_model(table_name)

            # Get model columns
            model_columns = {
                c.key: getattr(model, c.key) for c in model.__table__.columns
            }

            # Filter model columns to only those present in dataframe
            filter_columns = {k: v for k, v in model_columns.items() if k in dataframe.columns}

            for row_dict in dataframe.to_dicts():
                # Build filter dynamically for existing row check
                filters = [col == row_dict[col_name] for col_name, col in filter_columns.items()]

                # Query if this instance exists already
                exists = session.query(model).filter(sqlalchemy.and_(*filters)).first()

                if exists:
                    # Instance already in database
                    continue

                # Else add new instance
                instance = model(**row_dict)
                session.add(instance)

            session.flush()

        session.commit()


    @staticmethod
    def activate_listeners() -> None:
        # Register event listeners
        sqlalchemy.event.listen(sqlalchemy.orm.Session, "before_flush", ModelsConfig.block_inserts)

        for event_name in ["before_insert", "before_update"]:
            sqlalchemy.event.listen(Report.Report, event_name, ModelsConfig.listener_report_on_change)
            sqlalchemy.event.listen(Finisher.Finisher, event_name, ModelsConfig.listener_report_finisher_on_change)

            sqlalchemy.event.listen(Rate.Rate, event_name, ModelsConfig.listener_rate_on_change)
            sqlalchemy.event.listen(Finisher.Finisher, event_name, ModelsConfig.listener_rate_finisher_on_change)

            sqlalchemy.event.listen(StatementEntry.StatementEntry, event_name, ModelsConfig.listener_statement_entry_on_change)


    @staticmethod
    def block_inserts(
            session: sqlalchemy.orm.Session,
            flush_context: sqlalchemy.orm.unitofwork.UOWTransaction,
            instances: typing.Any
        ) -> None:
        for obj in session.new:
            if isinstance(obj, (Type.Type, FinisherPattern.FinisherPattern, StatementEntryPattern.StatementEntryPattern)):
                raise Exception(f"Inserts are not allowed for \"{getattr(obj.__class__, '__tablename__')}\".")


    @staticmethod
    def listener_report_on_change(
            mapper: sqlalchemy.orm.Mapper,
            connection: sqlalchemy.Connection,
            target: Report.Report,
            **kwargs: typing.Any
        ) -> None:
        if target.finishers:
            for finisher in target.finishers:
                ModelsConfig.listener_report_finisher_on_change(mapper, connection, finisher, parent_target = target, **kwargs)


    @staticmethod
    def listener_report_finisher_on_change(
            mapper: sqlalchemy.orm.Mapper,
            connection: sqlalchemy.Connection,
            target: Finisher.Finisher,
            **kwargs: typing.Any
        ) -> None:
        if not target.report_id:
            raise Exception("No \"report_id\" was given.")

        session: sqlalchemy.orm.Session = sqlalchemy.orm.session.object_session(target)

        report: Report.Report
        if "parent_target" in kwargs:
            report = kwargs["parent_target"]
        else:
            result: typing.Optional[Report.Report] = session.query(
                Report.Report
            ).where(
                Report.Report.id == target.report_id
            ).first()

            if not result:
                raise Exception("Unable to recover \"report\" values through \"report_id\".")

            report = result

        report_date: datetime.date = report.start_time.date()
        report_shift: int = report.shift

        finisher_patterns: typing.List[FinisherPattern.FinisherPattern] = sqlalchemy.orm.Session(
            bind = connection
        ).query(FinisherPattern.FinisherPattern).all()

        type_id: typing.Optional[str] = None
        payment_date: typing.Optional[datetime.date] = None
        for finisher_pattern in finisher_patterns:
            if re.match(finisher_pattern.pattern, target.name):
                type_id = finisher_pattern.type_id
                if finisher_pattern.payment_interval is not None:
                    payment_date = report_date + datetime.timedelta(days = finisher_pattern.payment_interval)

        # "cash" exception
        if type_id == "cash":
            payment_date += datetime.timedelta(days = 1 if report_shift > 0 else 0)

        # Fix payment day to next business day
        brazil_holidays: holidays.countries.brazil.BR = holidays.countries.brazil.BR()
        if payment_date:
            while not brazil_holidays.is_working_day(payment_date):
                payment_date += datetime.timedelta(days = 1)

        key: sqlalchemy.CursorResult = connection.execute(
            Finisher.Finisher.__table__.update().values(
                type_id = type_id,
                payment_date = payment_date
            ).where(
                Finisher.Finisher.id == target.id
            )
        )

        if not key in session.identity_map:
            sqlalchemy.orm.attributes.set_committed_value(target, "type_id", type_id)
            sqlalchemy.orm.attributes.set_committed_value(target, "payment_date", payment_date)


    @staticmethod
    def listener_rate_on_change(
            mapper: sqlalchemy.orm.Mapper,
            connection: sqlalchemy.Connection,
            target: Rate.Rate,
            **kwargs: typing.Any
        ) -> None:
        if target.type and target.type.finishers:
            for finisher in target.type.finishers:
                ModelsConfig.listener_rate_finisher_on_change(mapper, connection, finisher, parent_target = target, **kwargs)


    @staticmethod
    def listener_rate_finisher_on_change(
            mapper: sqlalchemy.orm.Mapper,
            connection: sqlalchemy.Connection,
            target: Finisher.Finisher,
            **kwargs: typing.Any
        ) -> None:
        if not target.type_id:
            return

        session: sqlalchemy.orm.Session = sqlalchemy.orm.session.object_session(target)

        rate_rate: float
        if "parent_target" in kwargs:
            rate_rate = kwargs["parent_target"].rate
        else:
            result: typing.Optional[Rate.Rate] = session.query(
                Rate.Rate
            ).where(
                Rate.Rate.type_id == target.type_id
            ).order_by(
                Rate.Rate.start_time.desc()
            ).first()

            if result:
                rate_rate = result.rate
            else:
                rate_rate = 0

        payment_value: int = math.trunc((target.value / 100) * (1 - rate_rate) * 100)

        key: sqlalchemy.CursorResult = connection.execute(
            Finisher.Finisher.__table__.update().values(
                payment_value = payment_value
            ).where(
                Finisher.Finisher.id == target.id
            )
        )

        if not key in session.identity_map:
            sqlalchemy.orm.attributes.set_committed_value(target, "payment_value", payment_value)


    @staticmethod
    def listener_statement_entry_on_change(
            mapper: sqlalchemy.orm.Mapper,
            connection: sqlalchemy.Connection,
            target: StatementEntry.StatementEntry,
            **kwargs: typing.Any
        ) -> None:
        session: sqlalchemy.orm.Session = sqlalchemy.orm.session.object_session(target)

        statement_entry_patterns: typing.List[StatementEntryPattern.StatementEntryPattern] = sqlalchemy.orm.Session(
            bind = connection
        ).query(
            StatementEntryPattern.StatementEntryPattern
        ).order_by(
            # Garantee null as last patterns
            StatementEntryPattern.StatementEntryPattern.pattern.asc()
        ).all()

        type_id: typing.Optional[str] = None
        for statement_entry_pattern in statement_entry_patterns:
            if re.match(statement_entry_pattern.value_pattern, str(target.value)) and \
            (not statement_entry_pattern.pattern or re.match(statement_entry_pattern.pattern, target.name)):
                type_id = statement_entry_pattern.type_id

        key: sqlalchemy.CursorResult = connection.execute(
            StatementEntry.StatementEntry.__table__.update().values(
                type_id = type_id,
            ).where(
                StatementEntry.StatementEntry.id == target.id
            )
        )

        if not key in session.identity_map:
            sqlalchemy.orm.attributes.set_committed_value(target, "type_id", type_id)