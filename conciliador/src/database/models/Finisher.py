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
    statement_entries: sqlalchemy.orm.Mapped[typing.List["StatementEntry"]] = sqlalchemy.orm.relationship( # type: ignore
        secondary = "finishers"
        back_populates = "finishers"
    )


# Register event listeners
@sqlalchemy.event.listens_for(Finisher, "before_insert")
@sqlalchemy.event.listens_for(Finisher, "before_update")
def validate_type_on_change(
        mapper: sqlalchemy.orm.Mapper,
        connection: sqlalchemy.Connection,
        target: Finisher
    ) -> None:
    if re.match("^RECEBIMENTO DINHEIRO(?!.*RECEITAS)$", target.name):
        target.type = ["cash"]
    elif re.match("^.*RECEITAS$", target.name):
        target.type = ["revenue"]
    elif re.match("^USO E CONSUMO$", target.name):
        target.type = ["usage_and_consumption"]
    elif re.match("^PRAZO$", target.name):
        target.type = ["installment"]
    elif re.match("^PIX.*$", target.name):
        target.type = ["pix"]
    elif re.match("^VISA CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "visa"]
    elif re.match("^MASTER CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "master"]
    elif re.match("^ELO CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "elo"]
    elif re.match("^HIPER$", target.name):
        target.type = ["card", "credit", "hipercard"]
    elif re.match("^.*AMEX$", target.name):
        target.type = ["card", "credit", "amex"]
    elif re.match("^PR[EÉ][ -]?PAGO VISA CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "visa", "prepaid"]
    elif re.match("^PR[EÉ][ -]?PAGO MASTER CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "master", "prepaid"]
    elif re.match("^PR[EÉ][ -]?PAGO ELO CR[EÉ]DITO$", target.name):
        target.type = ["card", "credit", "elo", "prepaid"]
    elif re.match("^(?:PR[EÉ][ -]?PAGO )?VISA D[EÉ]BITO$", target.name):
        target.type = ["card", "debit", "visa"]
    elif re.match("^(?:PR[EÉ][ -]?PAGO )?MASTER(?:CARD)? D[EÉ]BITO$", target.name):
        target.type = ["card", "debit", "master"]
    elif re.match("^(?:PR[EÉ][ -]?PAGO )?ELO D[EÉ]BITO$", target.name):
        target.type = ["card", "debit", "elo"]
    else:
        target.type = None