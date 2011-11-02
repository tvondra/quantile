/* a custom data type serving as a state for the quantile function */
CREATE TYPE quantile_state AS (
    vals        numeric[],
    quantiles   float[]
);

/* appends data to the quantile state, when there are multiple quantiles requested */
CREATE OR REPLACE FUNCTION quantile_append(p_state quantile_state, p_element numeric, p_quantiles float[]) RETURNS quantile_state
AS $$
BEGIN
    p_state.vals := array_append(p_state.vals, p_element);
    p_state.quantiles := p_quantiles;
    RETURN p_state;
END
$$ LANGUAGE plpgsql;

/* appends data to the quantile state, when there is a single quantile requested */
CREATE OR REPLACE FUNCTION quantile_append(p_state quantile_state, p_element numeric, p_quantile float) RETURNS quantile_state
AS $$
BEGIN
    p_state.vals := array_append(p_state.vals, p_element);
    p_state.quantiles := ARRAY[p_quantile];
    RETURN p_state;
END
$$ LANGUAGE plpgsql;

/* appends data to the quantile state, when the default quantile (median) is requested */
CREATE OR REPLACE FUNCTION quantile_append(p_state quantile_state, p_element numeric) RETURNS quantile_state
AS $$
BEGIN
    p_state.vals := array_append(p_state.vals, p_element);
    p_state.quantiles := ARRAY[0.5];
    RETURN p_state;
END
$$ LANGUAGE plpgsql;

/* processes the quantile state (at the end of aggregate evaluation) */
CREATE OR REPLACE FUNCTION quantile_final(p_state quantile_state) RETURNS numeric[]
AS $$
DECLARE
    v_result    numeric[];
    v_tmp       numeric[];
    v_length    int;
BEGIN

    SELECT ARRAY(
        SELECT p_state.vals[s.i] AS "foo"
        FROM
            generate_series(array_lower(p_state.vals,1), array_upper(p_state.vals,1)) AS s(i)
        ORDER BY foo
    ) INTO v_tmp;

    v_length := array_length(p_state.vals, 1);

    FOR v_idx IN 1..array_length(p_state.quantiles, 1) LOOP

        IF ((p_state.quantiles[v_idx] < 0) OR (p_state.quantiles[v_idx] > 1)) THEN
            RAISE EXCEPTION 'quantile % out of range [0,1]',p_state.quantiles[v_idx];
        END IF;

        v_result := array_append(v_result, v_tmp[ceil(v_length*p_state.quantiles[v_idx])]);
    END LOOP;

    RETURN v_result;

END
$$ LANGUAGE plpgsql;

/* processes the quantile state (at the end of aggregate evaluation) */
CREATE OR REPLACE FUNCTION quantile_single_final(p_state quantile_state) RETURNS numeric
AS $$
DECLARE
    v_tmp       numeric[];
    v_length    int;
BEGIN

    SELECT ARRAY(
        SELECT p_state.vals[s.i] AS "foo"
        FROM
            generate_series(array_lower(p_state.vals,1), array_upper(p_state.vals,1)) AS s(i)
        ORDER BY foo
    ) INTO v_tmp;

    v_length := array_length(p_state.vals, 1);

    IF ((p_state.quantiles[1] < 0) OR (p_state.quantiles[1] > 1)) THEN
        RAISE EXCEPTION 'quantile % out of range [0,1]',p_state.quantiles[1];
    END IF;

    RETURN v_tmp[ceil(v_length*p_state.quantiles[1])];

END
$$ LANGUAGE plpgsql;

/* returns array of requested quantiles */
CREATE AGGREGATE quantile(numeric, float[]) (
    SFUNC = quantile_append,
    STYPE = quantile_state,
    FINALFUNC = quantile_final
);

/* returns a single quantile */
CREATE AGGREGATE quantile(numeric, float) (
    SFUNC = quantile_append,
    STYPE = quantile_state,
    FINALFUNC = quantile_single_final
);

/* returns median */
CREATE AGGREGATE median(numeric) (
    SFUNC = quantile_append,
    STYPE = quantile_state,
    FINALFUNC = quantile_single_final
);
