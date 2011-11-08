MODULE_big = quantile
OBJS = src/quantile.o

EXTENSION = quantile
DATA = sql/quantile--1.0.sql sql/quantile--1.1.sql
MODULES = quantile

CFLAGS=`pg_config --includedir-server`

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

quantile.so: src/quantile.o

src/quantile.o: src/quantile.c
