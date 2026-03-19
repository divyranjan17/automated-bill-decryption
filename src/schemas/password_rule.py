from __future__ import annotations

from pydantic import BaseModel, field_validator
from typing import Optional
from enum import Enum


class AllowedField(str, Enum):
    name = "name"
    dob = "dob"
    mobile = "mobile"
    pan = "pan"
    card_masked = "card_masked"       # store last 8 digits only
    account_masked = "account_masked" # store last 8 digits only
    customer_id = "customer_id"


class Transform(str, Enum):
    upper = "upper"
    lower = "lower"
    none = "none"


class DateFormat(str, Enum):
    DDMM = "DDMM"
    MMDD = "MMDD"
    DDMMYY = "DDMMYY"
    DDMMYYYY = "DDMMYYYY"
    MMDDYYYY = "MMDDYYYY"
    YYYY = "YYYY"


class Confidence(str, Enum):
    high = "high"
    medium = "medium"
    low = "low"


class SliceRule(BaseModel):
    start: Optional[int]
    end: Optional[int]


class Component(BaseModel):
    field: AllowedField
    slice: Optional[SliceRule]
    transform: Optional[Transform]
    date_format: Optional[DateFormat]

class PasswordRuleVariant(BaseModel):
    """
    A single deterministic interpretation of an ambiguous rule.
    
    Used exclusively in PasswordRule.fallback_candidates.
    Cannot nest further — variants are always terminal.
    """
    components: list[Component]
    separator: str

class PasswordRule(BaseModel):
    components: list[Component]
    separator: str
    ambiguous: bool
    confidence: Confidence
    reasoning: str # Plain English explanation of what the LLM understood about the instruction
    # institution_hint: Optional[str] = None # dropped it for MVP
    requires_static_password: bool = False # in case it's OTP based and not derivable from email, set value True
    fallback_candidates: list[PasswordRuleVariant] = []
    unknown_fields: list[str] = []

    @field_validator("components")
    @classmethod
    def must_have_at_least_one(cls, v):
        if len(v) == 0:
            raise ValueError("components list cannot be empty")
        return v