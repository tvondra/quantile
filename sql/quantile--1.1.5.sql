/* quantile for the double precision */
CREATE OR REPLACE FUNCTION quantile_append_double(p_pointer internal, p_element double precision, p_quantile double precision)
    RETURNS internal
    AS 'quantile', 'quantile_append_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_append_double_array(p_pointer internal, p_element double precision, p_quantiles double precision[])
    RETURNS internal
    AS 'quantile', 'quantile_append_double_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_double(p_pointer internal)
    RETURNS double precision
    AS 'quantile', 'quantile_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_double_array(p_pointer internal)
    RETURNS double precision[]
    AS 'quantile', 'quantile_double_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_double_serial(p_pointer internal)
    RETURNS bytea
    AS 'quantile', 'quantile_double_serial'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_double_deserial(p_state bytea, p_dummy internal)
    RETURNS internal
    AS 'quantile', 'quantile_double_deserial'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_double_combine(p_state1 internal, p_state2 internal)
    RETURNS internal
    AS 'quantile', 'quantile_double_combine'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE quantile(double precision, double precision) (
    SFUNC = quantile_append_double,
    STYPE = internal,
    FINALFUNC = quantile_double,
    COMBINEFUNC = quantile_double_combine,
    SERIALFUNC = quantile_double_serial,
    DESERIALFUNC = quantile_double_deserial,
    PARALLEL = SAFE
);

CREATE AGGREGATE quantile(double precision, double precision[]) (
    SFUNC = quantile_append_double_array,
    STYPE = internal,
    FINALFUNC = quantile_double_array,
    COMBINEFUNC = quantile_double_combine,
    SERIALFUNC = quantile_double_serial,
    DESERIALFUNC = quantile_double_deserial,
    PARALLEL = SAFE
);

/* quantile for the numeric */
CREATE OR REPLACE FUNCTION quantile_append_numeric(p_pointer internal, p_element numeric, p_quantiles double precision)
    RETURNS internal
    AS 'quantile', 'quantile_append_numeric'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_append_numeric_array(p_pointer internal, p_element numeric, p_quantiles double precision[])
    RETURNS internal
    AS 'quantile', 'quantile_append_numeric_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_numeric(p_pointer internal)
    RETURNS numeric
    AS 'quantile', 'quantile_numeric'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_numeric_array(p_pointer internal)
    RETURNS numeric[]
    AS 'quantile', 'quantile_numeric_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_numeric_serial(p_pointer internal)
    RETURNS bytea
    AS 'quantile', 'quantile_numeric_serial'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_numeric_deserial(p_state bytea, p_dummy internal)
    RETURNS internal
    AS 'quantile', 'quantile_numeric_deserial'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_numeric_combine(p_state1 internal, p_state2 internal)
    RETURNS internal
    AS 'quantile', 'quantile_numeric_combine'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE quantile(numeric, double precision) (
    SFUNC = quantile_append_numeric,
    STYPE = internal,
    FINALFUNC = quantile_numeric,
    COMBINEFUNC = quantile_numeric_combine,
    SERIALFUNC = quantile_numeric_serial,
    DESERIALFUNC = quantile_numeric_deserial,
    PARALLEL = SAFE
);

CREATE AGGREGATE quantile(numeric, double precision[]) (
    SFUNC = quantile_append_numeric_array,
    STYPE = internal,
    FINALFUNC = quantile_numeric_array,
    COMBINEFUNC = quantile_numeric_combine,
    SERIALFUNC = quantile_numeric_serial,
    DESERIALFUNC = quantile_numeric_deserial,
    PARALLEL = SAFE
);

/* quantile for the int32 */
CREATE OR REPLACE FUNCTION quantile_append_int32(p_pointer internal, p_element int, p_quantile double precision)
    RETURNS internal
    AS 'quantile', 'quantile_append_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_append_int32_array(p_pointer internal, p_element int, p_quantiles double precision[])
    RETURNS internal
    AS 'quantile', 'quantile_append_int32_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int32(p_pointer internal)
    RETURNS int
    AS 'quantile', 'quantile_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int32_array(p_pointer internal)
    RETURNS int[]
    AS 'quantile', 'quantile_int32_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int32_serial(p_pointer internal)
    RETURNS bytea
    AS 'quantile', 'quantile_int32_serial'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int32_deserial(p_state bytea, p_dummy internal)
    RETURNS internal
    AS 'quantile', 'quantile_int32_deserial'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int32_combine(p_state1 internal, p_state2 internal)
    RETURNS internal
    AS 'quantile', 'quantile_int32_combine'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE quantile(int, double precision) (
    SFUNC = quantile_append_int32,
    STYPE = internal,
    FINALFUNC = quantile_int32,
    COMBINEFUNC = quantile_int32_combine,
    SERIALFUNC = quantile_int32_serial,
    DESERIALFUNC = quantile_int32_deserial,
    PARALLEL = SAFE
);

CREATE AGGREGATE quantile(int, double precision[]) (
    SFUNC = quantile_append_int32_array,
    STYPE = internal,
    FINALFUNC = quantile_int32_array,
    COMBINEFUNC = quantile_int32_combine,
    SERIALFUNC = quantile_int32_serial,
    DESERIALFUNC = quantile_int32_deserial,
    PARALLEL = SAFE
);

/* quantile for the int64 */
CREATE OR REPLACE FUNCTION quantile_append_int64(p_pointer internal, p_element bigint, p_quantile double precision)
    RETURNS internal
    AS 'quantile', 'quantile_append_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_append_int64_array(p_pointer internal, p_element bigint, p_quantiles double precision[])
    RETURNS internal
    AS 'quantile', 'quantile_append_int64_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int64(p_pointer internal)
    RETURNS bigint
    AS 'quantile', 'quantile_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int64_array(p_pointer internal)
    RETURNS bigint[]
    AS 'quantile', 'quantile_int64_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int64_serial(p_pointer internal)
    RETURNS bytea
    AS 'quantile', 'quantile_int64_serial'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int64_deserial(p_state bytea, p_dummy internal)
    RETURNS internal
    AS 'quantile', 'quantile_int64_deserial'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int64_combine(p_state1 internal, p_state2 internal)
    RETURNS internal
    AS 'quantile', 'quantile_int64_combine'
    LANGUAGE C IMMUTABLE;

/* actual aggregates */

CREATE AGGREGATE quantile(bigint, double precision) (
    SFUNC = quantile_append_int64,
    STYPE = internal,
    FINALFUNC = quantile_int64,
    COMBINEFUNC = quantile_int64_combine,
    SERIALFUNC = quantile_int64_serial,
    DESERIALFUNC = quantile_int64_deserial,
    PARALLEL = SAFE
);

CREATE AGGREGATE quantile(bigint, double precision[]) (
    SFUNC = quantile_append_int64_array,
    STYPE = internal,
    FINALFUNC = quantile_int64_array,
    COMBINEFUNC = quantile_int64_combine,
    SERIALFUNC = quantile_int64_serial,
    DESERIALFUNC = quantile_int64_deserial,
    PARALLEL = SAFE
);
