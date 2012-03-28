Quantile aggregates
===================
This extension provides three simple aggregate functions to compute
quantiles (http://en.wikipedia.org/wiki/Quantile). There are two
forms of aggregate functions available.


1) quantile(p_value numeric, p_quantile float)
----------------------------------------------
Computes arbitrary quantile of the values - the quantile has to be
between 0 and 1. For example this should return 500 just like the
previous example

    SELECT quantile(i, 0.5) FROM generate_series(1,1000) s(i);

but you can choose arbitrary quantile.


2) quantile(p_value numeric, p_quantiles float[])
-------------------------------------------------
If you need multiple quantiles at the same time (e.g. all four
quartiles), you can use this function instead of the one described
above. This version allows you to pass an array of quantiles and
returns an array of values.

So if you need all three quartiles, you may do this

    SELECT quantile(i, ARRAY[0.25, 0.5, 0.75])
     FROM generate_series(1,1000) s(i);

and it should return ARRAY[250, 500, 750]. Compared to calling
the simple quantile function like this

    SELECT quantile(i, 0.25), quantile(i, 0.5), quantile(i, 0.75)
     FROM generate_series(1,1000) s(i);

the advantage is that the values are collected just once (into
a single array), not for each expression separately. If you're
working with large data sets, this may save a lot of time and
memory (so it's a significant advantage).

Just as in the first case, thre are three functions handling other
basic numeric types, i.e. double, int (32-bit) and bigint (64-bit).


Installation
------------
Installing this is very simple - if you're on 9.1 you can install
it like any other extension, i.e.

    $ make install
    $ psql dbname -c "CREATE EXTENSION quantile"

and if you're on an older version, you have to run the SQL script
manually

    $ psql dbname < `pg_config --sharedir`/contrib/quantile--1.1.sql

That's all.


License
-------
This software is distributed under the terms of BSD 2-clause license.
See LICENSE or http://www.opensource.org/licenses/bsd-license.php for
more details.
