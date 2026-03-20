"""Pydantic models and Pandera schemas for the carts endpoint."""

import pandera as pa
from pandera import Check, Column, DataFrameSchema
from pydantic import BaseModel, ConfigDict


# Pydantic models


class CartProduct(BaseModel):
    productId: int
    quantity: int


class CartModel(BaseModel):
    model_config = ConfigDict(extra="allow")  # captures __v

    id: int
    userId: int
    date: str
    products: list[CartProduct]


# Pandera schemas

silver_carts_schema = DataFrameSchema(
    columns={
        "id": Column(int, nullable=False),
        "userId": Column(int, nullable=False),
        "productId": Column(int, nullable=False),
        "quantity": Column(int, nullable=False, checks=Check.greater_than(0)),
        "date": Column(pa.DateTime, nullable=False),
        "_ingested_at": Column(pa.DateTime, nullable=False),
    },
    strict=False,
)

gold_carts_schema = DataFrameSchema(
    columns={
        "id": Column(int, nullable=False, checks=Check.greater_than(0)),
        "userId": Column(
            int,
            nullable=False,
            checks=Check.greater_than(0),
        ),
        "productId": Column(int, nullable=False, checks=Check.greater_than(0)),
        "quantity": Column(
            int,
            nullable=False,
            checks=Check.greater_than(0),
        ),
        "date": Column(pa.DateTime, nullable=False),
        "_ingested_at": Column(pa.DateTime, nullable=False),
    },
    strict=False,
)
