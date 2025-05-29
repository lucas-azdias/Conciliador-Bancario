import sqlalchemy
import sqlalchemy.orm
import re
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class StatementEntry(BaseModel.BaseModel):

    # Table name
    __tablename__ = "statement_entry"


    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        unique = True,
        nullable = False,
        autoincrement = True
    )
    statement_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("statement.id"),
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


    # Computed columns
    @sqlalchemy.ext.hybrid.hybrid_property
    def verified_value(self) -> int:
        return sum(finisher.value for finisher in self.finishers)


    # Relationships
    statement: sqlalchemy.orm.Mapped["Statement"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "statement_entries"
    )
    verification: sqlalchemy.orm.Mapped["Verification"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "statement_entries"
    )


# Register event listeners
@sqlalchemy.event.listens_for(StatementEntry, "before_insert")
@sqlalchemy.event.listens_for(StatementEntry, "before_update")
def validate_type_on_change(
        mapper: sqlalchemy.orm.Mapper,
        connection: sqlalchemy.Connection,
        target: StatementEntry
    ) -> None:
    if re.match("^PIX CREDITO(?!.*TRR IVAI COMERCIO DE COMBUST).*$", target.name) or \
    (re.match("^TRANSFERENCIA.*$", target.name) and re.match("^\\d+$", str(target.value))):
        target.type = ["pix"]
    elif re.match("^DEPÓSITO.*$", target.name):
        target.type = ["deposit"]
    elif re.match("^VENDAS CARTAO TIPO CREDITO.*CIELO-VISA.*$", target.name):
        target.type = ["card", "credit", "visa"]
    elif re.match("^VENDAS CARTAO TIPO CREDITO.*CIELO-MASTER.*$", target.name):
        target.type = ["card", "credit", "master"]
    elif re.match("^VENDAS CARTAO TIPO CREDITO.*CIELO-ELO.*$", target.name):
        target.type = ["card", "credit", "elo"]
    elif re.match("^VENDAS CARTAO TIPO CREDITO.*CIELO-HIPERCA.*$", target.name):
        target.type = ["card", "credit", "hipercard"]
    elif re.match("^VENDAS CARTAO TIPO CREDITO.*CIELO-AMERICA.*$", target.name):
        target.type = ["card", "credit", "amex"]
    elif re.match("^VENDAS CARTAO TIPO DEBITO.*CIELO-VISA.*$", target.name):
        target.type = ["card", "debit", "visa"]
    elif re.match("^VENDAS CARTAO TIPO DEBITO.*CIELO-MAESTRO.*$", target.name):
        target.type = ["card", "debit", "master"]
    elif re.match("^VENDAS CARTAO TIPO DEBITO.*CIELO-ELO.*$", target.name):
        target.type = ["card", "debit", "elo"]
    elif re.match("^\\d+$", str(target.value)):
        target.type = ["income"]
    elif re.match("^-\\d+$", str(target.value)):
        target.type = ["outcome"]
    else:
        target.type = None