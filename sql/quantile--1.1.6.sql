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

CREATE AGGREGATE quantile(double precision, double precision) (
    SFUNC = quantile_append_double,
    STYPE = internal,
    FINALFUNC = quantile_double
);

CREATE AGGREGATE quantile(double precision, double precision[]) (
    SFUNC = quantile_append_double_array,
    STYPE = internal,
    FINALFUNC = quantile_double_array
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

CREATE AGGREGATE quantile(numeric, double precision) (
    SFUNC = quantile_append_numeric,
    STYPE = internal,
    FINALFUNC = quantile_numeric
);

CREATE AGGREGATE quantile(numeric, double precision[]) (
    SFUNC = quantile_append_numeric_array,
    STYPE = internal,
    FINALFUNC = quantile_numeric_array
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

CREATE AGGREGATE quantile(int, double precision) (
    SFUNC = quantile_append_int32,
    STYPE = internal,
    FINALFUNC = quantile_int32
);

CREATE AGGREGATE quantile(int, double precision[]) (
    SFUNC = quantile_append_int32_array,
    STYPE = internal,
    FINALFUNC = quantile_int32_array
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

/* actual aggregates */

CREATE AGGREGATE quantile(bigint, double precision) (
    SFUNC = quantile_append_int64,
    STYPE = internal,
    FINALFUNC = quantile_int64
);

CREATE AGGREGATE quantile(bigint, double precision[]) (
    SFUNC = quantile_append_int64_array,
    STYPE = internal,
    FINALFUNC = quantile_int64_array
);
