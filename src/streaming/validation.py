"""Pure PySpark DataFrame validator enforcing the cpc_event.v1 parity contract (Task 3.3).

Mirrors ``src/common/schemas/cpc_event.py:validate_event`` and
``src/common/schemas/validation_rules.md`` without Python UDFs, including raw-JSON type
checks so numeric JSON amounts (e.g. ``"spend": 12.5``) are caught as
``malformed_decimal`` even after ``from_json`` string coercion.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

ALLOWED_FIELDS: tuple[str, ...] = (
    "schema_version",
    "event_id",
    "event_time",
    "produced_at",
    "event_type",
    "job_id",
    "campaign_id",
    "group_id",
    "publisher_id",
    "bid",
    "spend",
    "source_row_id",
)

VALID_EVENT_TYPES: tuple[str, ...] = ("click", "apply", "qualified", "unqualified")

REQUIRED_NON_EMPTY_STRING_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_time",
    "produced_at",
    "event_type",
    "job_id",
    "campaign_id",
    "publisher_id",
    "spend",
)

_STRICT_STRING_ID_FIELDS: tuple[str, ...] = ("event_id", "job_id", "campaign_id", "publisher_id")

_UTC_TS_REGEX = (
    r"^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])"
    r"T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|\+00:00)$"
)
_DECIMAL_STR_REGEX = r"^[+-]?\d+(?:\.\d+)?$"
_MAX_DECIMAL_BOUND = "100000000000000"  # 10^14 (DECIMAL(18,4) integer limit)


def get_event_json_schema() -> Any:
    """Return explicit Spark StructType schema for permissive JSON wire parsing."""
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            StructField("schema_version", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("produced_at", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("job_id", StringType(), True),
            StructField("campaign_id", StringType(), True),
            StructField("group_id", StringType(), True),
            StructField("publisher_id", StringType(), True),
            StructField("bid", StringType(), True),
            StructField("spend", StringType(), True),
            StructField("source_row_id", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )


def _is_json_string(payload_col: Column, field_name: str) -> Column:
    """Return True if field_name's raw JSON token starts with a double quote."""
    return payload_col.rlike(rf'(?:^|[{{,])\s*"{field_name}"\s*:\s*"')


def _is_json_integer(payload_col: Column, field_name: str) -> Column:
    """Return True if field_name's raw JSON token is an unquoted integer literal."""
    return payload_col.rlike(rf'(?:^|[{{,])\s*"{field_name}"\s*:\s*-?\d+\s*(?:[,}}]|$)')


def _is_missing_string_value(val_col: Column) -> Column:
    """Return True when value is NULL or blank whitespace."""
    from pyspark.sql import functions as F

    return val_col.isNull() | (F.trim(val_col) == F.lit(""))


def _invalid_timestamp_cond(payload_col: Column, val_col: Column, field_name: str) -> Column:
    """Return True when timestamp field fails ISO-8601 UTC format or calendar validity."""
    from pyspark.sql import functions as F

    valid_date = F.expr(f"try_cast(substring({field_name}, 1, 10) as date)").isNotNull()
    return (
        ~_is_json_string(payload_col, field_name)
        | ~val_col.rlike(_UTC_TS_REGEX)
        | ~valid_date
    )


def _decimal_error_cond(
    payload_col: Column, val_col: Column, field_name: str
) -> tuple[Column, Column]:
    """Return (is_malformed, is_negative) conditions matching Python _parse_decimal_amount."""
    from pyspark.sql import functions as F

    trimmed = F.trim(val_col)
    is_str = _is_json_string(payload_col, field_name)
    matches_syntax = (trimmed != F.lit("")) & trimmed.rlike(_DECIMAL_STR_REGEX)
    as_wide_dec = trimmed.cast("decimal(38, 10)")
    is_negative = is_str & matches_syntax & as_wide_dec.isNotNull() & (as_wide_dec < F.lit(0))

    frac_len = F.when(
        trimmed.contains("."),
        F.length(F.regexp_extract(trimmed, r"\.(\d+)$", 1)),
    ).otherwise(F.lit(0))

    out_of_bounds = (
        (frac_len > F.lit(4))
        | as_wide_dec.isNull()
        | (F.abs(as_wide_dec) >= F.lit(_MAX_DECIMAL_BOUND).cast("decimal(38, 10)"))
    )
    is_malformed = ~is_str | ~matches_syntax | (~is_negative & out_of_bounds)
    return is_malformed, is_negative


def validate_df(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Validate raw Kafka/JSON rows and split into ``(valid_df, invalid_df)``.

    Input DataFrame must contain ``payload_raw`` (STRING). Optional Kafka metadata columns
    (``kafka_topic``, ``kafka_partition``, ``kafka_offset``, ``kafka_timestamp``) are
    preserved when present and defaulted to typed NULLs in standalone tests.
    """
    from pyspark.sql import functions as F

    schema = get_event_json_schema()
    parse_opts = {
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": "_corrupt_record",
        "allowNonNumericNumbers": "false",
    }
    map_opts = {
        "mode": "PERMISSIVE",
        "allowNonNumericNumbers": "false",
    }

    cols = set(df.columns)
    enriched = df
    if "kafka_topic" not in cols:
        enriched = enriched.withColumn("kafka_topic", F.lit(None).cast("string"))
    if "kafka_partition" not in cols:
        enriched = enriched.withColumn("kafka_partition", F.lit(None).cast("int"))
    if "kafka_offset" not in cols:
        enriched = enriched.withColumn("kafka_offset", F.lit(None).cast("long"))
    if "kafka_timestamp" not in cols:
        enriched = enriched.withColumn("kafka_timestamp", F.lit(None).cast("timestamp"))

    payload_col = F.col("payload_raw")
    trimmed_payload = F.trim(payload_col)

    parsed_df = (
        enriched.withColumn("_parsed", F.from_json(payload_col, schema, parse_opts))
        .withColumn("_raw_map", F.from_json(payload_col, "MAP<STRING, STRING>", map_opts))
        .select(
            "payload_raw",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "_raw_map",
            "_parsed.*",
        )
    )

    # 1. corrupt_json
    is_corrupt = (
        payload_col.isNull()
        | (trimmed_payload == F.lit(""))
        | ~trimmed_payload.startswith("{")
        | ~trimmed_payload.endswith("}")
        | F.col("_corrupt_record").isNotNull()
        | F.col("_raw_map").isNull()
    )

    # 2. schema_version
    sv_col = F.col("schema_version")
    sv_missing = _is_missing_string_value(sv_col)
    sv_unsupported = ~sv_missing & (
        ~_is_json_string(payload_col, "schema_version") | (sv_col != F.lit("v1"))
    )

    # 3. required non-empty strings
    req_missing_cond = F.lit(False)
    for req_field in REQUIRED_NON_EMPTY_STRING_FIELDS:
        req_missing_cond = req_missing_cond | _is_missing_string_value(F.col(req_field))
    for str_field in _STRICT_STRING_ID_FIELDS:
        f_col = F.col(str_field)
        req_missing_cond = req_missing_cond | (
            ~_is_missing_string_value(f_col) & ~_is_json_string(payload_col, str_field)
        )
    event_id_too_long = (
        ~_is_missing_string_value(F.col("event_id"))
        & _is_json_string(payload_col, "event_id")
        & (F.length(F.col("event_id")) > F.lit(64))
    )

    # 4. source_row_id
    row_id_str = F.col("source_row_id")
    row_id_missing_or_type = row_id_str.isNull() | ~_is_json_integer(payload_col, "source_row_id")
    row_id_num = F.when(~row_id_missing_or_type, row_id_str.cast("bigint"))
    row_id_negative = ~row_id_missing_or_type & (row_id_num < F.lit(0))

    # 5. extra unknown fields (additionalProperties: false)
    allowed_array = F.array(*[F.lit(name) for name in ALLOWED_FIELDS])
    has_extra_fields = F.size(F.array_except(F.map_keys(F.col("_raw_map")), allowed_array)) > F.lit(0)

    # 6. optional group_id non-string
    group_id_bad_type = F.col("group_id").isNotNull() & ~_is_json_string(payload_col, "group_id")

    # 7. event_type enum
    et_col = F.col("event_type")
    et_missing = _is_missing_string_value(et_col)
    et_bad_enum = ~et_missing & (
        ~_is_json_string(payload_col, "event_type") | ~et_col.isin(*VALID_EVENT_TYPES)
    )

    # 8. conditional bid for click
    bid_col = F.col("bid")
    bid_missing_for_click = (
        _is_json_string(payload_col, "event_type")
        & (et_col == F.lit("click"))
        & _is_missing_string_value(bid_col)
    )

    # 9. UTC timestamps
    et_ts_missing = _is_missing_string_value(F.col("event_time"))
    pa_ts_missing = _is_missing_string_value(F.col("produced_at"))
    bad_event_time = ~et_ts_missing & _invalid_timestamp_cond(
        payload_col, F.col("event_time"), "event_time"
    )
    bad_produced_at = ~pa_ts_missing & _invalid_timestamp_cond(
        payload_col, F.col("produced_at"), "produced_at"
    )

    # 10. decimals (spend, bid)
    spend_missing = _is_missing_string_value(F.col("spend"))
    spend_malformed, spend_negative = _decimal_error_cond(payload_col, F.col("spend"), "spend")
    spend_malformed = ~spend_missing & spend_malformed
    spend_negative = ~spend_missing & spend_negative

    bid_present = bid_col.isNotNull() & ~bid_missing_for_click
    bid_malformed_raw, bid_negative_raw = _decimal_error_cond(payload_col, bid_col, "bid")
    bid_malformed = bid_present & bid_malformed_raw
    bid_negative = bid_present & bid_negative_raw

    # Priority order strictly aligned with Python validate_event() primary_error_type
    error_type_col = (
        F.when(is_corrupt, F.lit("corrupt_json"))
        .when(sv_missing, F.lit("missing_required"))
        .when(sv_unsupported, F.lit("schema_version_unsupported"))
        .when(req_missing_cond | event_id_too_long, F.lit("missing_required"))
        .when(row_id_missing_or_type, F.lit("missing_required"))
        .when(row_id_negative, F.lit("negative_amount"))
        .when(has_extra_fields | group_id_bad_type, F.lit("missing_required"))
        .when(et_bad_enum, F.lit("bad_enum"))
        .when(bid_missing_for_click, F.lit("missing_required"))
        .when(bad_event_time | bad_produced_at, F.lit("bad_timestamp"))
        .when(spend_malformed, F.lit("malformed_decimal"))
        .when(spend_negative, F.lit("negative_amount"))
        .when(bid_malformed, F.lit("malformed_decimal"))
        .when(bid_negative, F.lit("negative_amount"))
        .otherwise(F.lit(None).cast("string"))
    )

    error_detail_col = (
        F.when(is_corrupt, F.lit("Invalid or non-object JSON payload"))
        .when(sv_missing, F.lit("Missing required field 'schema_version'"))
        .when(sv_unsupported, F.lit("Unsupported schema_version; expected 'v1'"))
        .when(event_id_too_long, F.lit("Field 'event_id' exceeds maximum length 64"))
        .when(req_missing_cond, F.lit("Missing, empty, or non-string required field"))
        .when(row_id_missing_or_type, F.lit("Field 'source_row_id' missing or not integer"))
        .when(row_id_negative, F.lit("Field 'source_row_id' must be >= 0"))
        .when(has_extra_fields, F.lit("Unexpected fields not permitted by schema v1"))
        .when(group_id_bad_type, F.lit("Optional field 'group_id' must be string or null"))
        .when(et_bad_enum, F.lit("Invalid event_type outside allowed funnel enum"))
        .when(bid_missing_for_click, F.lit("Field 'bid' is required when event_type == 'click'"))
        .when(bad_event_time, F.lit("Invalid UTC ISO-8601 timestamp in 'event_time'"))
        .when(bad_produced_at, F.lit("Invalid UTC ISO-8601 timestamp in 'produced_at'"))
        .when(spend_malformed, F.lit("Invalid string-encoded DECIMAL(18,4) in 'spend'"))
        .when(spend_negative, F.lit("Field 'spend' must be non-negative"))
        .when(bid_malformed, F.lit("Invalid string-encoded DECIMAL(18,4) in 'bid'"))
        .when(bid_negative, F.lit("Field 'bid' must be non-negative"))
        .otherwise(F.lit(None).cast("string"))
    )

    classified = parsed_df.withColumn("error_type", error_type_col).withColumn(
        "error_detail", error_detail_col
    )

    valid_df = (
        classified.filter(F.col("error_type").isNull())
        .select(
            F.col("schema_version"),
            F.col("event_id"),
            F.col("event_time"),
            F.col("produced_at"),
            F.col("event_type"),
            F.col("job_id"),
            F.col("campaign_id"),
            F.col("group_id"),
            F.col("publisher_id"),
            F.col("bid"),
            F.col("spend"),
            F.col("source_row_id").cast("bigint").alias("source_row_id"),
            F.col("payload_raw"),
            F.col("kafka_topic"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("kafka_timestamp"),
            F.current_timestamp().alias("ingested_at"),
            F.expr("try_cast(substring(event_time, 1, 10) as date)").alias("event_date"),
        )
    )

    invalid_df = (
        classified.filter(F.col("error_type").isNotNull())
        .select(
            F.col("payload_raw"),
            F.col("error_type"),
            F.col("error_detail"),
            F.col("kafka_topic").alias("source_topic"),
            F.col("kafka_partition").alias("source_partition"),
            F.col("kafka_offset").alias("source_offset"),
            F.current_timestamp().alias("failed_at"),
            F.lit("ingest").alias("stage"),
        )
    )

    return valid_df, invalid_df
