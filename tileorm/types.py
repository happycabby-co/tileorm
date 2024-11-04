from typing import NamedTuple
from pydantic import AliasChoices, Field


class Point(NamedTuple):
    lat: float = Field(validation_alias=AliasChoices("latitude", "lat"))
    lon: float = Field(validation_alias=AliasChoices("longitude", "lon"))


class Bounds(NamedTuple):
    minlat: float = Field(validation_alias=AliasChoices("minlatitude", "minlat"))
    minlon: float = Field(validation_alias=AliasChoices("minlongitude", "minlon"))
    maxlat: float = Field(validation_alias=AliasChoices("maxlatitude", "maxlat"))
    maxlon: float = Field(validation_alias=AliasChoices("maxlongitude", "maxlon"))
