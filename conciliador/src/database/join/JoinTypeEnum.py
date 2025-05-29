import enum
import typeguard


@typeguard.typechecked
class JoinTypeEnum(enum.Enum):
    INNER = enum.auto()
    LEFT_OUTER = enum.auto()
    RIGHT_OUTER = enum.auto()
    FULL_OUTER = enum.auto()
    CROSS = enum.auto()