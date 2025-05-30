import datetime
import sqlalchemy
import sqlalchemy.ext.hybrid
import sqlalchemy.orm
import re
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class Finisher(BaseModel.BaseModel):

    # Table name
    __tablename__ = "finisher"


    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        unique = True,
        nullable = False,
        autoincrement = True
    )
    report_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("report.id"),
        nullable = False
    )
    verification_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("verification.id"),
        nullable = True
    )
    name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        nullable = False
    )
    value: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        nullable = False
    )
    type: sqlalchemy.orm.Mapped[typing.Optional[typing.List[str]]] = sqlalchemy.orm.mapped_column(
        sqlalchemy.JSON,
        nullable = True
    )
    payment_date: sqlalchemy.orm.Mapped[datetime.date] = sqlalchemy.orm.mapped_column(
        nullable = True
    )


    # Constraints
    __table_args__ = (
        sqlalchemy.UniqueConstraint("report_id", "name", name = "unique_report_id_type"),
    )


    # Computed columns
    @sqlalchemy.ext.hybrid.hybrid_property
    def verified_value(self) -> int:
        return sum(statement_entry.value for statement_entry in self.statement_entries)


    # Relationships
    report: sqlalchemy.orm.Mapped["Report"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "finishers"
    )
    verification: sqlalchemy.orm.Mapped["Verification"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "finishers"
    )


# Register event listeners
@typeguard.typechecked
@sqlalchemy.event.listens_for(Finisher, "before_insert")
@sqlalchemy.event.listens_for(Finisher, "before_update")
def validate_on_change(
        mapper: sqlalchemy.orm.Mapper,
        connection: sqlalchemy.Connection,
        target: Finisher
    ) -> None:
    if not target.report_id:
        raise Exception("No \"report_id\" was given.")

    reports_table: sqlalchemy.Table = sqlalchemy.table(
        "report",
        sqlalchemy.column("id"),
        sqlalchemy.column("shift"),
        sqlalchemy.column("start_time")
    )

    query: sqlalchemy.Select = sqlalchemy.select(reports_table.c.start_time, reports_table.c.shift).where(reports_table.c.id == target.report_id)
    result: sqlalchemy.Row = connection.execute(query).first()

    if not result:
        raise Exception("Unable to recover \"report\" values through \"report_id\".")

    report_start_time, report_shift = result
    report_date = datetime.datetime.strptime(report_start_time, "%Y-%m-%d %H:%M:%S.%f").date()

    if re.match("^RECEBIMENTO DINHEIRO(?!.*RECEITAS)$", target.name):
        target.type = ["cash"]
        target.payment_date = report_date + datetime.timedelta(days = (1 if report_shift > 0 else 0))
    elif re.match("^.*RECEITAS$", target.name):
        target.type = ["revenue"]
        target.payment_date = None
    elif re.match("^USO E CONSUMO$", target.name):
        target.type = ["usage_and_consumption"]
        target.payment_date = None
    elif re.match("^PRAZO$", target.name):
        target.type = ["installment"]
        target.payment_date = None
    elif re.match("^PIX.*$", target.name):
        target.type = ["pix"]
        target.payment_date = report_date
    elif re.match("^VISA CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "visa"]
        target.payment_date = report_date + datetime.timedelta(days = 30)
    elif re.match("^MASTER CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "master"]
        target.payment_date = report_date + datetime.timedelta(days = 30)
    elif re.match("^ELO CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "elo"]
        target.payment_date = report_date + datetime.timedelta(days = 30)
    elif re.match("^HIPER$", target.name):
        target.type = ["card", "credit", "hipercard"]
        target.payment_date = report_date + datetime.timedelta(days = 30)
    elif re.match("^.*AMEX$", target.name):
        target.type = ["card", "credit", "amex"]
        target.payment_date = report_date + datetime.timedelta(days = 30)
    elif re.match("^PR[EÉ][ -]?PAGO VISA CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "visa"]
        target.payment_date = report_date + datetime.timedelta(days = 2)
    elif re.match("^PR[EÉ][ -]?PAGO MASTER CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "master"]
        target.payment_date = report_date + datetime.timedelta(days = 2)
    elif re.match("^PR[EÉ][ -]?PAGO ELO CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "elo"]
        target.payment_date = report_date + datetime.timedelta(days = 2)
    elif re.match("^VISA D[EÉ]BITO$", target.name):
        target.type = ["card", "debit", "visa"]
        target.payment_date = report_date + datetime.timedelta(days = 1)
    elif re.match("^MASTER(?:CARD)? D[EÉ]BITO$", target.name):
        target.type = ["card", "debit", "master"]
        target.payment_date = report_date + datetime.timedelta(days = 1)
    elif re.match("^ELO D[EÉ]BITO$", target.name):
        target.type = ["card", "debit", "elo"]
        target.payment_date = report_date + datetime.timedelta(days = 1)
    elif re.match("^PR[EÉ][ -]?PAGO VISA D[EÉ]BITO$", target.name):
        target.type = ["card", "debit", "visa"]
        target.payment_date = report_date + datetime.timedelta(days = 1)
    elif re.match("^PR[EÉ][ -]?PAGO MASTER(?:CARD)? D[EÉ]BITO$", target.name):
        target.type = ["card", "debit", "master"]
        target.payment_date = report_date + datetime.timedelta(days = 1)
    elif re.match("^PR[EÉ][ -]?PAGO ELO D[EÉ]BITO$", target.name):
        target.type = ["card", "debit", "elo"]
        target.payment_date = report_date + datetime.timedelta(days = 1)
    else:
        target.type = None
        target.payment_date = None