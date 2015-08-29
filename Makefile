MODULE_big = quantile
OBJS = src/quantile.o

EXTENSION = quantile
DATA = sql/quantile--1.1.5.sql sql/quantile--1.1.4--1.1.5.sql
MODULES = quantile

CFLAGS=`pg_config --includedir-server`

TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

quantile.so: src/quantile.o

src/quantile.o: src/quantile.c
