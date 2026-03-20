"""Pydantic models and Pandera schemas for the users endpoint."""

import pandera as pa
from pandera import Check, Column, DataFrameSchema
from pydantic import BaseModel, ConfigDict


# Pydantic models


class UserGeolocation(BaseModel):
    lat: str
    long: str


class UserAddress(BaseModel):
    geolocation: UserGeolocation
    city: str
    street: str
    number: int
    zipcode: str


class UserName(BaseModel):
    firstname: str
    lastname: str


class UserModel(BaseModel):
    model_config = ConfigDict(extra="allow")  # captures __v and any
    # other extra fields without validation errors

    id: int
    email: str
    username: str
    password: str
    phone: str
    name: UserName
    address: UserAddress


# Pandera schemas

# HMAC-SHA256 output is always exactly 64 hex characters
_HMAC_CHECK = Check.str_length(min_value=64, max_value=64)

silver_users_schema = DataFrameSchema(
    columns={
        "id": Column(int, nullable=False),
        "email": Column(str, nullable=False, checks=_HMAC_CHECK),
        "username": Column(str, nullable=False, checks=_HMAC_CHECK),
        "name_firstname": Column(str, nullable=False, checks=_HMAC_CHECK),
        "name_lastname": Column(str, nullable=False, checks=_HMAC_CHECK),
        "phone": Column(str, nullable=False, checks=_HMAC_CHECK),
        "address_city": Column(str, nullable=True, required=False),
        "address_zipcode": Column(str, nullable=True, required=False),
        "_ingested_at": Column(pa.DateTime, nullable=False),
    },
    strict=False,  # allow extra columns (like __v) without failing
)

gold_users_schema = DataFrameSchema(
    columns={
        "id": Column(int, nullable=False, checks=Check.greater_than(0)),
        "email": Column(str, nullable=False, checks=_HMAC_CHECK),
        "username": Column(str, nullable=False, checks=_HMAC_CHECK),
        "name_firstname": Column(str, nullable=False, checks=_HMAC_CHECK),
        "name_lastname": Column(str, nullable=False, checks=_HMAC_CHECK),
        "phone": Column(str, nullable=False, checks=_HMAC_CHECK),
        "address_city": Column(str, nullable=True, required=False),
        "address_zipcode": Column(str, nullable=True, required=False),
        "_ingested_at": Column(pa.DateTime, nullable=False),
    },
    strict=False,
)
