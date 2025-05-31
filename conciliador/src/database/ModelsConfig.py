import datetime
import math
import re
import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .models import Finisher, Rate, Report, StatementEntry, Type


@typeguard.typechecked
class ModelsConfig():

    @staticmethod
    def setup_models(
            session: sqlalchemy.orm.Session
        ) -> None:
        # Remove all Type instances
        session.query(Type.Type).delete()
        session.commit()

        # Insert all new Type instances
        session.add_all(
            [
                Type.Type(id = type_id, name = type_name)
                for type_id, type_name in enumerate(Type.TYPES_NAMES, start = 1)
            ]
        )
        session.commit()


    @staticmethod
    def activate_listeners() -> None:
        # Register event listeners
        for event_name in ["before_insert", "before_update"]:
            sqlalchemy.event.listen(Report.Report, event_name, ModelsConfig.listener_report_on_change)
            sqlalchemy.event.listen(Finisher.Finisher, event_name, ModelsConfig.listener_report_finisher_on_change)

            sqlalchemy.event.listen(Rate.Rate, event_name, ModelsConfig.listener_rate_on_change)
            sqlalchemy.event.listen(Finisher.Finisher, event_name, ModelsConfig.listener_rate_finisher_on_change)

            sqlalchemy.event.listen(StatementEntry.StatementEntry, event_name, ModelsConfig.listener_statement_entry_on_change)


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

        name: str = target.name

        type_id: typing.Optional[int]
        payment_date: typing.Optional[datetime.date]
        if re.match("^RECEBIMENTO DINHEIRO(?!.*RECEITAS)$", name):
            type_id = Type.TYPES_NAMES.index("cash") + 1
            payment_date = report_date + datetime.timedelta(days = (1 if report_shift > 0 else 0))
        elif re.match("^.*RECEITAS$", name):
            type_id = Type.TYPES_NAMES.index("revenue") + 1
            payment_date = None
        elif re.match("^USO E CONSUMO$", name):
            type_id = Type.TYPES_NAMES.index("usage_and_consumption") + 1
            payment_date = None
        elif re.match("^PRAZO$", name):
            type_id = Type.TYPES_NAMES.index("installment") + 1
            payment_date = None
        elif re.match("^PIX.*$", name):
            type_id = Type.TYPES_NAMES.index("pix") + 1
            payment_date = report_date

        elif re.match("^VISA CR[EÉ]DITO$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.visa") + 1
            payment_date = report_date + datetime.timedelta(days = 30)
        elif re.match("^MASTER CR[EÉ]DITO$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.master") + 1
            payment_date = report_date + datetime.timedelta(days = 30)
        elif re.match("^ELO CR[EÉ]DITO$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.elo") + 1
            payment_date = report_date + datetime.timedelta(days = 30)
        elif re.match("^HIPER$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.hipercard") + 1
            payment_date = report_date + datetime.timedelta(days = 30)
        elif re.match("^.*AMEX$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.amex") + 1
            payment_date = report_date + datetime.timedelta(days = 30)

        elif re.match("^PR[EÉ][ -]?PAGO VISA CR[EÉ]DITO$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.visa") + 1
            payment_date = report_date + datetime.timedelta(days = 2)
        elif re.match("^PR[EÉ][ -]?PAGO MASTER CR[EÉ]DITO$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.master") + 1
            payment_date = report_date + datetime.timedelta(days = 2)
        elif re.match("^PR[EÉ][ -]?PAGO ELO CR[EÉ]DITO$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.elo") + 1
            payment_date = report_date + datetime.timedelta(days = 2)

        elif re.match("^VISA D[EÉ]BITO$", name):
            type_id = Type.TYPES_NAMES.index("card.debit.visa") + 1
            payment_date = report_date + datetime.timedelta(days = 1)
        elif re.match("^MASTER(?:CARD)? D[EÉ]BITO$", name):
            type_id = Type.TYPES_NAMES.index("card.debit.master") + 1
            payment_date = report_date + datetime.timedelta(days = 1)
        elif re.match("^ELO D[EÉ]BITO$", name):
            type_id = Type.TYPES_NAMES.index("card.debit.elo") + 1
            payment_date = report_date + datetime.timedelta(days = 1)

        elif re.match("^PR[EÉ][ -]?PAGO VISA D[EÉ]BITO$", name):
            type_id = Type.TYPES_NAMES.index("card.debit.visa") + 1
            payment_date = report_date + datetime.timedelta(days = 1)
        elif re.match("^PR[EÉ][ -]?PAGO MASTER(?:CARD)? D[EÉ]BITO$", name):
            type_id = Type.TYPES_NAMES.index("card.debit.master") + 1
            payment_date = report_date + datetime.timedelta(days = 1)
        elif re.match("^PR[EÉ][ -]?PAGO ELO D[EÉ]BITO$", name):
            type_id = Type.TYPES_NAMES.index("card.debit.elo") + 1
            payment_date = report_date + datetime.timedelta(days = 1)

        else:
            type_id = None
            payment_date = None

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

        rate: Rate.Rate
        if "parent_target" in kwargs:
            rate = kwargs["parent_target"]
        else:
            result: typing.Optional[Rate.Rate] = session.query(
                Rate.Rate
            ).where(
                Rate.Rate.type_id == target.type_id
            ).order_by(
                Rate.Rate.start_time.desc()
            ).first()

            if not result:
                return

            rate = result

        payment_value: int = math.trunc((target.value / 100) * (1 - rate.rate) * 100)

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

        name: str = target.name
        str_value: str = str(target.value)

        type_id: typing.Optional[int]
        if re.match("^DEPÓSITO.*$", name):
            type_id = Type.TYPES_NAMES.index("cash") + 1
        elif re.match("^PIX CREDITO(?!.*TRR IVAI COMERCIO DE COMBUST).*$", name) or \
        (re.match("^TRANSFERENCIA.*$", name) and re.match("^\\d+$", str_value)):
            type_id = Type.TYPES_NAMES.index("pix") + 1

        elif re.match("^VENDAS CARTAO TIPO CREDITO.*CIELO-VISA.*$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.visa") + 1
        elif re.match("^VENDAS CARTAO TIPO CREDITO.*CIELO-MASTER.*$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.master") + 1
        elif re.match("^VENDAS CARTAO TIPO CREDITO.*CIELO-ELO.*$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.elo") + 1
        elif re.match("^VENDAS CARTAO TIPO CREDITO.*CIELO-HIPERCA.*$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.hipercard") + 1
        elif re.match("^VENDAS CARTAO TIPO CREDITO.*CIELO-AMERICA.*$", name):
            type_id = Type.TYPES_NAMES.index("card.credit.amex") + 1

        elif re.match("^VENDAS CARTAO TIPO DEBITO.*CIELO-VISA.*$", name):
            type_id = Type.TYPES_NAMES.index("card.debit.visa") + 1
        elif re.match("^VENDAS CARTAO TIPO DEBITO.*CIELO-MAESTRO.*$", name):
            type_id = Type.TYPES_NAMES.index("card.debit.master") + 1
        elif re.match("^VENDAS CARTAO TIPO DEBITO.*CIELO-ELO.*$", name):
            type_id = Type.TYPES_NAMES.index("card.debit.elo") + 1

        elif re.match("^\\d+$", str_value):
            type_id = Type.TYPES_NAMES.index("income") + 1
        elif re.match("^-\\d+$", str_value):
            type_id = Type.TYPES_NAMES.index("outcome") + 1

        else:
            type_id = None

        key: sqlalchemy.CursorResult = connection.execute(
            StatementEntry.StatementEntry.__table__.update().values(
                type_id = type_id,
            ).where(
                StatementEntry.StatementEntry.id == target.id
            )
        )

        if not key in session.identity_map:
            sqlalchemy.orm.attributes.set_committed_value(target, "type_id", type_id)