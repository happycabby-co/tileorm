class TileOrmException(Exception): ...


class NoIdentifier(TileOrmException):
    def __init__(self, name: str, *args):
        super().__init__(
            f"No identifier found for value '{name}'. "
            f"Ensure '{name}' has an 'Identifier()' field.",
            *args,
        )


class MultipleIdentifiers(TileOrmException):
    def __init__(self, name: str, *args):
        super().__init__(
            f"Multiple identifiers found for value '{name}'. "
            f"Check the amount of 'Identifier()' fields declared on '{name}'.",
            *args,
        )


class NoLocation(TileOrmException):
    def __init__(self, name: str, *args):
        super().__init__(
            f"No location found for value '{name}'. "
            f"Ensure '{name}' has a 'Location()' field.",
            *args,
        )


class MultipleLocations(TileOrmException):
    def __init__(self, name: str, *args):
        super().__init__(
            f"Multiple locations found for value '{name}'. "
            f"Check the amount of 'Location()' fields declared on '{name}'.",
            *args,
        )


class NotFoundError(TileOrmException):
    def __init__(self, name: str, key: str, id: str, *args):
        super().__init__(
            f"Object of type '{name}' not found in collection '{key}' with id '{id}'.",
            *args,
        )
