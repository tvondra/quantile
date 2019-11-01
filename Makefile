MODULE_big = quantile
OBJS = quantile.o

EXTENSION = quantile
DATA = sql/quantile--1.1.7.sql sql/quantile--1.1.4--1.1.5.sql sql/quantile--1.1.5--1.1.6.sql sql/quantile--1.1.6--1.1.7.sql
MODULES = quantile

CFLAGS=`pg_config --includedir-server`

TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
