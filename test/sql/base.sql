BEGIN;

CREATE EXTENSION quantile;

-- int
SELECT quantile(x, 0.5) FROM generate_series(1,1000) s(x);
SELECT quantile(x, 0.1) FROM generate_series(1,1000) s(x);
SELECT quantile(x, 0) FROM generate_series(1,1000) s(x);
SELECT quantile(x, 1) FROM generate_series(1,1000) s(x);

SELECT quantile(x, ARRAY[0.5]) FROM generate_series(1,1000) s(x);
SELECT quantile(x, ARRAY[0.1]) FROM generate_series(1,1000) s(x);
SELECT quantile(x, ARRAY[0]) FROM generate_series(1,1000) s(x);
SELECT quantile(x, ARRAY[1]) FROM generate_series(1,1000) s(x);

SELECT quantile(x, ARRAY[0, 0.1, 0.5, 0.75, 1]) FROM generate_series(1,1000) s(x);

-- bigint
SELECT quantile(x::bigint, 0.5) FROM generate_series(1,1000) s(x);
SELECT quantile(x::bigint, 0.1) FROM generate_series(1,1000) s(x);
SELECT quantile(x::bigint, 0) FROM generate_series(1,1000) s(x);
SELECT quantile(x::bigint, 1) FROM generate_series(1,1000) s(x);

SELECT quantile(x::bigint, ARRAY[0.5]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::bigint, ARRAY[0.1]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::bigint, ARRAY[0]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::bigint, ARRAY[1]) FROM generate_series(1,1000) s(x);

SELECT quantile(x::bigint, ARRAY[0, 0.1, 0.5, 0.75, 1]) FROM generate_series(1,1000) s(x);

-- double precision
SELECT quantile(x::double precision, 0.5) FROM generate_series(1,1000) s(x);
SELECT quantile(x::double precision, 0.1) FROM generate_series(1,1000) s(x);
SELECT quantile(x::double precision, 0) FROM generate_series(1,1000) s(x);
SELECT quantile(x::double precision, 1) FROM generate_series(1,1000) s(x);

SELECT quantile(x::double precision, ARRAY[0.5]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::double precision, ARRAY[0.1]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::double precision, ARRAY[0]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::double precision, ARRAY[1]) FROM generate_series(1,1000) s(x);

SELECT quantile(x::double precision, ARRAY[0, 0.1, 0.5, 0.75, 1]) FROM generate_series(1,1000) s(x);

-- numeric
SELECT quantile(x::numeric, 0.5) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, 0.1) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, 0) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, 1) FROM generate_series(1,1000) s(x);

SELECT quantile(x::numeric, ARRAY[0.5]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, ARRAY[0.1]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, ARRAY[0]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, ARRAY[1]) FROM generate_series(1,1000) s(x);

SELECT quantile(x::numeric, ARRAY[0, 0.1, 0.5, 0.75, 1]) FROM generate_series(1,1000) s(x);

ROLLBACK;
