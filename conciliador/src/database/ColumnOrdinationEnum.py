import enum
import typeguard


@typeguard.typechecked
class ColumnOrdinationEnum(enum.StrEnum):
    ASCENDING = "asc"
    DESCENDING = "desc"