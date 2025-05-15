import pathlib
import polars
import typeguard
import typing

from . import Loader


REPORT_COLUMNS = ("Turno", "Funcionário", "Data", "Início", "Término", "Finalizadora", "Total")


class ReportLoader(Loader.Loader):

    @typeguard.typechecked
    def __init__(
            self,
            path_filter: str,
            input: pathlib.Path,
            archive: typing.Optional[pathlib.Path] = None,
        ) -> None:
        super().__init__(
            path_filter = path_filter,
            input = input,
            archive = archive,
        )


    @typeguard.typechecked
    def process_file(
            self,
            path: pathlib.Path,
            encoding: typing.Optional[str] = None
        ) -> polars.DataFrame:
        report_content = open(path, encoding = encoding or self.detect_encoding(path)).read()

        # Remove unuseful header
        report_content = report_content.split("\n", maxsplit = 5)[-1]

        # Remove unuseful footer
        report_content = report_content.rsplit("\n", maxsplit = 3)[0]

        # Split the content into sections by the first element and filter last columns
        sections: typing.List[typing.List[typing.List[str]]] = list()
        rows = report_content.split("\n")
        for row in rows:
            columns = row.split(";")[:-5]

            if columns[0].strip(): # If it is a new report section
                sections.append([columns])
                continue

            sections[-1].append(columns)

        df_sections: typing.List[polars.DataFrame] = list()
        start_times: typing.List[str] = list()
        for i, section in enumerate(sections):        
            info, _, section, _ = section[0], section[1], section[2:-1], section[-1]

            # Get the info
            name = info[0]
            date, start_time = info[3].split(" ")[:2]
            _, end_time = info[4].split(" ")[:2]
            start_times.append(start_time)

            # Repeat info through section
            for j in range(len(section)):
                finisher = section[j][1]
                value = section[j][4] if finisher != "RECEBIMENTO DINHEIRO" else section[j][5]
                section[j] = [
                    "",
                    name,
                    date,
                    start_time,
                    end_time,
                    finisher,
                    value,
                ]

            # Convert the sections into DataFrames and store them
            df_sections.append(polars.DataFrame(section, schema = REPORT_COLUMNS, orient = "row"))

        # Using start time to define shift order
        starts = sorted([df_section.item(0, "Início") for df_section in df_sections])
        df_sections.sort(key = lambda x: starts.index(x.item(0, "Início")))

        # Combine all dataframes and insert shift value
        report = polars.DataFrame(schema = {x: str for x in REPORT_COLUMNS})
        for i, df_section in enumerate(df_sections):
            df_section = df_section.with_columns([(polars.lit(str(i))).alias("Turno")])
            report.vstack(df_section, in_place = True)

        return report