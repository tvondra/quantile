/* quantile for the double precision */
CREATE OR REPLACE FUNCTION quantile_append_double(p_pointer bigint, p_element double precision, p_quantile double precision)
    RETURNS bigint
    AS 'quantile', 'quantile_append_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_append_double_array(p_pointer bigint, p_element double precision, p_quantiles double precision[])
    RETURNS bigint
    AS 'quantile', 'quantile_append_double_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_double(p_pointer bigint)
    RETURNS double precision
    AS 'quantile', 'quantile_double'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_double_array(p_pointer bigint)
    RETURNS double precision[]
    AS 'quantile', 'quantile_double_array'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE quantile(double precision, double precision) (
    SFUNC = quantile_append_double,
    STYPE = bigint,
    FINALFUNC = quantile_double
);

CREATE AGGREGATE quantile(double precision, double precision[]) (
    SFUNC = quantile_append_double_array,
    STYPE = bigint,
    FINALFUNC = quantile_double_array
);

/* quantile for the int32 */
CREATE OR REPLACE FUNCTION quantile_append_int32(p_pointer bigint, p_element int, p_quantile double precision)
    RETURNS bigint
    AS 'quantile', 'quantile_append_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_append_int32_array(p_pointer bigint, p_element int, p_quantiles double precision[])
    RETURNS bigint
    AS 'quantile', 'quantile_append_int32_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int32(p_pointer bigint)
    RETURNS int
    AS 'quantile', 'quantile_int32'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int32_array(p_pointer bigint)
    RETURNS int[]
    AS 'quantile', 'quantile_int32_array'
    LANGUAGE C IMMUTABLE;

CREATE AGGREGATE quantile(int, double precision) (
    SFUNC = quantile_append_int32,
    STYPE = bigint,
    FINALFUNC = quantile_int32
);

CREATE AGGREGATE quantile(int, double precision[]) (
    SFUNC = quantile_append_int32_array,
    STYPE = bigint,
    FINALFUNC = quantile_int32_array
);

/* quantile for the int64 */
CREATE OR REPLACE FUNCTION quantile_append_int64(p_pointer bigint, p_element bigint, p_quantile double precision)
    RETURNS bigint
    AS 'quantile', 'quantile_append_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_append_int64_array(p_pointer bigint, p_element bigint, p_quantiles double precision[])
    RETURNS bigint
    AS 'quantile', 'quantile_append_int64_array'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int64(p_pointer bigint)
    RETURNS bigint
    AS 'quantile', 'quantile_int64'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION quantile_int64_array(p_pointer bigint)
    RETURNS bigint[]
    AS 'quantile', 'quantile_int64_array'
    LANGUAGE C IMMUTABLE;

/* actual aggregates */

CREATE AGGREGATE quantile(bigint, double precision) (
    SFUNC = quantile_append_int64,
    STYPE = bigint,
    FINALFUNC = quantile_int64
);

CREATE AGGREGATE quantile(bigint, double precision[]) (
    SFUNC = quantile_append_int64_array,
    STYPE = bigint,
    FINALFUNC = quantile_int64_array
);
