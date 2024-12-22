# Quantile aggregates

[![make installcheck](https://github.com/tvondra/quantile/actions/workflows/ci.yml/badge.svg)](https://github.com/tvondra/quantile/actions/workflows/ci.yml)
[![PGXN version](https://badge.fury.io/pg/quantile.svg)](https://badge.fury.io/pg/quantile)

This extension provides three simple aggregate functions to compute
quantiles (http://en.wikipedia.org/wiki/Quantile). There are two
forms of aggregate functions available - the first one returns
a single quantile, the second one returns an arbitrary number of
quantiles (as an array).


## History

This extension was created in 2011, before PostgreSQL added functions to
compute percentiles (`percentile_cont` and `percentile_disc`) in 9.4,
which was released in December 2014. Even after introduction of those
built-in functions it made sense to use this extension, because it was
significantly faster in various cases.

The performance of the built-in functions improved a lot since then, and
is usually very close or even faster than this extension. In some cases
the extension is perhaps 2x faster than the built-in functions, but that
may be (at least partially) attributed to not respecting `work_mem`.

It's therefore recommended to evaluate the built-in functions first, and
only use this extension if it's provably (and consistently) faster than
the built-in functions and the risk of running out of memory is low, or
when it's necessary to support older PostgreSQL releases (pre-9.4) that
do not have the built-in alternatives.


## `quantile(p_value numeric, p_quantile float)`

Computes arbitrary quantile of the values - the p_quantile has to be
between 0 and 1. For example this should return 500 because 500 is the
median of a sequence 1 .. 1000.

```
SELECT quantile(i, 0.5) FROM generate_series(1,1000) s(i);
```

but you can choose arbitrary quantile (for example 0.95).

This function is overloaded for the four basic numeric types, i.e.
`int`, `bigint`, `double precision` and `numeric`.


## `quantile(p_value numeric, p_quantiles float[])`

If you need multiple quantiles at the same time (e.g. all four
quartiles), you can use this function instead of the one described
above. This version allows you to pass an array of quantiles and
returns an array of values.

So if you need all three quartiles, you may do this

```
SELECT quantile(i, ARRAY[0.25, 0.5, 0.75])
  FROM generate_series(1,1000) s(i);
```

and it should return ARRAY[250, 500, 750]. Compared to calling
the simple quantile function like this

```
SELECT quantile(i, 0.25), quantile(i, 0.5), quantile(i, 0.75)
 FROM generate_series(1,1000) s(i);
```

the advantage is that the values are collected just once (into
a single array), not for each expression separately. If you're
working with large data sets, this may save a significant amount
of time and memory (if may even be the factor that allows the query
to finish and not being killed by OOM killer or something).

Just as in the first case, there are four functions handling other
basic numeric types: `int`, `bigint`, `double precision` and `numeric`.


## Installation

Installing this is very simple, especially if you're using pgxn client.
All you need to do is this:

    $ pgxn install quantile
    $ pgxn load -d mydb quantile

and you're done. You may also install the extension manually:

    $ make install
    $ psql dbname -c "CREATE EXTENSION quantile"

And if you're on an older version (pre-9.1), you have to run the SQL
script manually

    $ psql dbname < `pg_config --sharedir`/contrib/quantile--1.1.7.sql

That's all.


## License

This software is distributed under the terms of BSD 2-clause license.
See LICENSE or http://www.opensource.org/licenses/bsd-license.php for
more details.
