from sqlalchemy import create_engine, Column
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import set_committed_value
from datetime import datetime, date
from conciliador.src.database.models import Report, Statement, StatementEntry, Finisher, Rate, Type
from conciliador.src.database import BaseModel, ModelsConfig

# Create engine and tables
engine = create_engine("sqlite:///:memory:", echo=False)
BaseModel.BaseModel.metadata.create_all(engine)
ModelsConfig.ModelsConfig.setup_models(Session(engine))
ModelsConfig.ModelsConfig.activate_listeners()

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

    # Create Rates
    rate1 = Rate.Rate(rate=0.02, start_time=datetime(2000, 1, 1), type_id=1)
    session.add_all([rate1])
    session.commit()

    # Create Finishers
    finisher1 = Finisher.Finisher(report_id=report1.id, name="RECEBIMENTO DINHEIRO", value=595700)
    finisher2 = Finisher.Finisher(report_id=report1.id, name="ELO DEBITO", value=38500)
    finisher3 = Finisher.Finisher(report_id=report2.id, name="VISA DEBITO", value=282617)
    session.add_all([finisher1, finisher2, finisher3])
    session.commit()

    # Query all tables
    print("\nAll Reports:")
    for report in session.query(Report.Report).all():
        print(f"Report(id={report.id}, shift={report.shift}, employee={report.employee}, "
              f"start_time={report.start_time}, end_time={report.end_time})")

    print("\nAll Rates:")
    for rate in session.query(Rate.Rate).all():
        print(f"Rate(id={rate.id}, rate={rate.rate}, start_time={rate.start_time}, "
              f"type={rate.type})")

    print("\nAll Finishers:")
    for finisher in session.query(Finisher.Finisher).all():
       print(f"Finisher(id={finisher.id}, report_id={finisher.report_id}, name={finisher.name}, "
              f"value={finisher.value}, type={finisher.type}, payment_date={finisher.payment_date}, payment_value={finisher.payment_value})")

    print("\nINNER JOIN Results (Report and Finisher):")
    query = session.query(Report.Report, Finisher.Finisher).join(Finisher.Finisher, Report.Report.id == Finisher.Finisher.report_id)
    results = query.all()
    print(results)


with Session(engine) as session:
    finisher2 = session.get(Finisher.Finisher, 2)
    finisher2.name = "ELO CRÉDITO"
    rate1 = session.get(Rate.Rate, 1)
    rate1.rate = 0.05
    session.commit()

    print("\nAll Rates:")
    for rate in session.query(Rate.Rate).all():
        print(f"Rate(id={rate.id}, rate={rate.rate}, start_time={rate.start_time}, "
              f"type={rate.type})")

    print("\nAll Finishers:")
    for finisher in session.query(Finisher.Finisher).all():
       print(f"Finisher(id={finisher.id}, report_id={finisher.report_id}, name={finisher.name}, "
              f"value={finisher.value}, type={finisher.type}, payment_date={finisher.payment_date}, payment_value={finisher.payment_value})")