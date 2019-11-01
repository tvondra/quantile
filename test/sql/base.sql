\set ECHO none

-- disable the notices for the create script (shell types etc.)
SET client_min_messages = 'WARNING';
\i sql/quantile--1.1.7.sql
SET client_min_messages = 'NOTICE';

\set ECHO all

-- int
SELECT quantile(x, -0.5) FROM generate_series(1,1000) s(x);
SELECT quantile(x, 1.5) FROM generate_series(1,1000) s(x);
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
SELECT quantile(x::bigint, -0.5) FROM generate_series(1,1000) s(x);
SELECT quantile(x::bigint, 1.5) FROM generate_series(1,1000) s(x);
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
SELECT quantile(x::double precision, -0.5) FROM generate_series(1,1000) s(x);
SELECT quantile(x::double precision, 1.5) FROM generate_series(1,1000) s(x);
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
SELECT quantile(x::numeric, -0.5) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, 1.5) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, 0.5) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, 0.1) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, 0) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, 1) FROM generate_series(1,1000) s(x);

SELECT quantile(x::numeric, ARRAY[0.5]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, ARRAY[0.1]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, ARRAY[0]) FROM generate_series(1,1000) s(x);
SELECT quantile(x::numeric, ARRAY[1]) FROM generate_series(1,1000) s(x);

SELECT quantile(x::numeric, ARRAY[0, 0.1, 0.5, 0.75, 1]) FROM generate_series(1,1000) s(x);

-- test of correct NULL handling (skipping with all NULLS)
CREATE TABLE parent_table (id int);
CREATE TABLE child_table  (id int, val int);

INSERT INTO parent_table SELECT i FROM generate_series(1,10) s(i);
INSERT INTO child_table  SELECT 2, i FROM generate_series(1,100) s(i);
INSERT INTO child_table  SELECT 4, i FROM generate_series(1,100) s(i);
INSERT INTO child_table  SELECT 6, i FROM generate_series(1,100) s(i);
INSERT INTO child_table  SELECT 8, i FROM generate_series(1,100) s(i);
INSERT INTO child_table  SELECT 10, i FROM generate_series(1,100) s(i);

SELECT parent_table.id, quantile(child_table.val, array[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]) FROM parent_table LEFT JOIN child_table USING (id) GROUP BY id ORDER BY id;
SELECT parent_table.id, quantile(child_table.val, 0.5) FROM parent_table LEFT JOIN child_table USING (id) GROUP BY id ORDER BY id;

SELECT parent_table.id, quantile(child_table.val::bigint, array[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]) FROM parent_table LEFT JOIN child_table USING (id) GROUP BY id ORDER BY id;
SELECT parent_table.id, quantile(child_table.val::bigint, 0.5) FROM parent_table LEFT JOIN child_table USING (id) GROUP BY id ORDER BY id;

SELECT parent_table.id, quantile(child_table.val::double precision, array[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]) FROM parent_table LEFT JOIN child_table USING (id) GROUP BY id ORDER BY id;
SELECT parent_table.id, quantile(child_table.val::double precision, 0.5) FROM parent_table LEFT JOIN child_table USING (id) GROUP BY id ORDER BY id;

SELECT parent_table.id, quantile(child_table.val::numeric, array[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]) FROM parent_table LEFT JOIN child_table USING (id) GROUP BY id ORDER BY id;
SELECT parent_table.id, quantile(child_table.val::numeric, 0.5) FROM parent_table LEFT JOIN child_table USING (id) GROUP BY id ORDER BY id;

-- test of correct NULL handling (skipping with mixed NULL and not-NULL values)
TRUNCATE child_table;
INSERT INTO child_table  SELECT i, (CASE WHEN MOD(i,2) = 0 THEN i/2 ELSE NULL END) FROM generate_series(1,200) s(i);

SELECT quantile(val, array[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]) FROM (SELECT val FROM child_table ORDER BY id) AS foo;
SELECT quantile(val::bigint, array[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]) FROM (SELECT val FROM child_table ORDER BY id) AS foo;
SELECT quantile(val::double precision, array[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]) FROM (SELECT val FROM child_table ORDER BY id) AS foo;
SELECT quantile(val::numeric, array[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]) FROM (SELECT val FROM child_table ORDER BY id) AS foo;
