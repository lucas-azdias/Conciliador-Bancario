import typeguard


@typeguard.typechecked
class Currency():

    def __init__(
            self,
            currency: str,
            thousands: str = ",",
            decimals: str = "."
        ) -> None:
        self.__currency: str = currency
        self.__thousands: str = thousands
        self.__decimals: str = decimals

    def format_money(
            self,
            value: int | str,
            include_currency: bool = True
        ) -> str:
        # Check for invalid characters
        if isinstance(value, str) and not value.isdigit():
            raise ValueError("Invalid \"value\" given. It contains non-digit characters.")

        # Convert to string and standard size
        money = str(value).zfill(3)

        # Format thousands
        thousands = "{:,}".format(int(money[0:-2])).replace(",", self.__thousands)

        # Format money
        money = thousands + self.__decimals + money[-2:]

        # Append currency
        if include_currency:
            money = self.__currency + " " + money

        return money


if __name__ == "__main__":
    c = Currency("R$", thousands = ".", decimals = ",")
    print(c.format_money("012830123"))