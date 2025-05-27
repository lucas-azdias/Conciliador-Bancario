from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from datetime import datetime, date
from conciliador.src.database.models import Report, Statement, StatementEntry, Finisher, StatementEntryFinisherLink
from conciliador.src.database import BaseModel

# Create engine and tables
engine = create_engine("sqlite:///:memory:", echo=False)
BaseModel.BaseModel.metadata.create_all(engine)

# Create sample data and query all tables
with Session(engine) as session:
    # Create Reports
    report1 = Report.Report(
        shift=0,
        employee="EMILY",
        start_time=datetime(2025, 2, 19, 6, 0, 40),
        end_time=datetime(2025, 2, 19, 13, 0, 59)
    )
    report2 = Report.Report(
        shift=1,
        employee="JOAO LOPES",
        start_time=datetime(2025, 2, 19, 13, 1, 25),
        end_time=datetime(2025, 2, 19, 20, 8, 4)
    )
    session.add_all([report1, report2])
    session.commit()

    # Create Finishers
    finisher1 = Finisher.Finisher(report_id=report1.id, name="RECEBIMENTO DINHEIRO", value=595700, type="RECEBIMENTO DINHEIRO")
    finisher2 = Finisher.Finisher(report_id=report1.id, name="ELO DEBITO", value=38500, type="ELO DEBITO")
    finisher3 = Finisher.Finisher(report_id=report2.id, name="VISA DEBITO", value=282617, type="VISA DEBITO")
    session.add_all([finisher1, finisher2, finisher3])
    session.commit()

    # Create Statements and StatementEntries
    statement = Statement.Statement(date=date(2025,2,2))
    statement_entry1 = StatementEntry.StatementEntry(statement_id=1, name="DEPÓSITO", value=13, type=None)
    statement_entry2 = StatementEntry.StatementEntry(statement_id=2, name="PIX CREDITO: LUCIO", value=45, type=None)
    session.add_all([statement, statement_entry1, statement_entry2])
    session.commit()

    # Link StatementEntry and Finisher
    statement_entry1.finishers.append(finisher1)
    statement_entry1.finishers.append(finisher3)
    statement_entry2.finishers.append(finisher1)
    session.commit()

    # Query all tables
    print("\nAll Reports:")
    for report in session.query(Report.Report).all():
        print(f"Report(id={report.id}, shift={report.shift}, employee={report.employee}, "
              f"start_time={report.start_time}, end_time={report.end_time})")

    print("\nAll Finishers:")
    for finisher in session.query(Finisher.Finisher).all():
        print(f"Finisher(id={finisher.id}, report_id={finisher.report_id}, name={finisher.name}, "
              f"value={finisher.value}, type={finisher.type}) [verified_value={finisher.verified_value}]")

    print("\nAll Statements:")
    for statement in session.query(Statement.Statement).all():
        print(f"Statement(id={statement.id}, date={statement.date})")

    print("\nAll Statement Entries:")
    for entry in session.query(StatementEntry.StatementEntry).all():
        print(f"StatementEntry(id={entry.id}, statement_id={entry.statement_id}, name={entry.name}, value={entry.value}, type={entry.type})")

    print("\nAll Statement Entry Finisher Links:")
    for link in session.query(StatementEntryFinisherLink.StatementEntryFinisherLink).all():
        print(f"Link(statement_entry_id={link.statement_entry_id}, finisher_id={link.finisher_id})")