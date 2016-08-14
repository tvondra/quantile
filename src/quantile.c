/*
* quantile.c - Quantile aggregate function
* Copyright (C) Tomas Vondra, 2011
*
*/

#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <limits.h>

#include "postgres.h"
#include "utils/datum.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "nodes/execnodes.h"
#include "access/tupmacs.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define GET_AGG_CONTEXT(fname, fcinfo, aggcontext)  \
    if (! AggCheckCallContext(fcinfo, &aggcontext)) {   \
        elog(ERROR, "%s called in non-aggregate context", fname);  \
    }

#define CHECK_AGG_CONTEXT(fname, fcinfo)  \
    if (! AggCheckCallContext(fcinfo, NULL)) {   \
        elog(ERROR, "%s called in non-aggregate context", fname);  \
    }

/* Structures used to keep the data - the 'elements' array is extended
 * on the fly if needed. */

typedef struct struct_double {

    int nquantiles;
    int nelements;
    int next;
    bool sorted;

    double * quantiles;
    double * elements;

} struct_double;

typedef struct struct_int32 {

    int nquantiles;
    int nelements;
    int next;
    bool sorted;

    double * quantiles;
    int32  * elements;

} struct_int32;

typedef struct struct_int64 {

    int nquantiles;
    int nelements;
    int next;
    bool sorted;

    double * quantiles;
    int64  * elements;

} struct_int64;

typedef struct struct_numeric {

    int nquantiles;
    int next;

    int maxlen;     /* total size of the buffer */
    int usedlen;    /* used part of the buffer */

    double * quantiles;
    char   * data;  /* contents of the numeric values */

} struct_numeric;

static void sort_state_double(struct_double *state);
static void sort_state_int32(struct_int32 *state);
static void sort_state_int64(struct_int64 *state);
static void sort_state_numeric(struct_numeric *state);

/* comparators, used for qsort */

static int  double_comparator(const void *a, const void *b);
static int  int32_comparator(const void *a, const void *b);
static int  int64_comparator(const void *a, const void *b);
static int  numeric_comparator(const void *a, const void *b);

/* parse the quantiles array */
static double *
array_to_double(FunctionCallInfo fcinfo, ArrayType *v, int * len);

static Datum
double_to_array(FunctionCallInfo fcinfo, double * d, int len);

static Datum
int32_to_array(FunctionCallInfo fcinfo, int32 * d, int len);

static Datum
int64_to_array(FunctionCallInfo fcinfo, int64 * d, int len);

static Datum
numeric_to_array(FunctionCallInfo fcinfo, Numeric * d, int len);

static void
check_quantiles(int nquantiles, double * quantiles);

/* prototypes */
PG_FUNCTION_INFO_V1(quantile_append_double_array);
PG_FUNCTION_INFO_V1(quantile_append_double);

PG_FUNCTION_INFO_V1(quantile_double_array);
PG_FUNCTION_INFO_V1(quantile_double);

PG_FUNCTION_INFO_V1(quantile_double_serial);
PG_FUNCTION_INFO_V1(quantile_double_deserial);
PG_FUNCTION_INFO_V1(quantile_double_combine);

PG_FUNCTION_INFO_V1(quantile_append_int32_array);
PG_FUNCTION_INFO_V1(quantile_append_int32);

PG_FUNCTION_INFO_V1(quantile_int32_array);
PG_FUNCTION_INFO_V1(quantile_int32);

PG_FUNCTION_INFO_V1(quantile_int32_serial);
PG_FUNCTION_INFO_V1(quantile_int32_deserial);
PG_FUNCTION_INFO_V1(quantile_int32_combine);

PG_FUNCTION_INFO_V1(quantile_append_int64_array);
PG_FUNCTION_INFO_V1(quantile_append_int64);

PG_FUNCTION_INFO_V1(quantile_int64_array);
PG_FUNCTION_INFO_V1(quantile_int64);

PG_FUNCTION_INFO_V1(quantile_int64_serial);
PG_FUNCTION_INFO_V1(quantile_int64_deserial);
PG_FUNCTION_INFO_V1(quantile_int64_combine);

PG_FUNCTION_INFO_V1(quantile_append_numeric_array);
PG_FUNCTION_INFO_V1(quantile_append_numeric);

PG_FUNCTION_INFO_V1(quantile_numeric_array);
PG_FUNCTION_INFO_V1(quantile_numeric);

Datum quantile_append_double_array(PG_FUNCTION_ARGS);
Datum quantile_append_double(PG_FUNCTION_ARGS);

Datum quantile_double_array(PG_FUNCTION_ARGS);
Datum quantile_double(PG_FUNCTION_ARGS);

Datum quantile_double_serial(PG_FUNCTION_ARGS);
Datum quantile_double_deserial(PG_FUNCTION_ARGS);
Datum quantile_double_combine(PG_FUNCTION_ARGS);

Datum quantile_append_int32_array(PG_FUNCTION_ARGS);
Datum quantile_append_int32(PG_FUNCTION_ARGS);

Datum quantile_int32_array(PG_FUNCTION_ARGS);
Datum quantile_int32(PG_FUNCTION_ARGS);

Datum quantile_int32_serial(PG_FUNCTION_ARGS);
Datum quantile_int32_deserial(PG_FUNCTION_ARGS);
Datum quantile_int32_combine(PG_FUNCTION_ARGS);

Datum quantile_append_int64_array(PG_FUNCTION_ARGS);
Datum quantile_append_int64(PG_FUNCTION_ARGS);

Datum quantile_int64_array(PG_FUNCTION_ARGS);
Datum quantile_int64(PG_FUNCTION_ARGS);

Datum quantile_int64_serial(PG_FUNCTION_ARGS);
Datum quantile_int64_deserial(PG_FUNCTION_ARGS);
Datum quantile_int64_combine(PG_FUNCTION_ARGS);

Datum quantile_append_numeric_array(PG_FUNCTION_ARGS);
Datum quantile_append_numeric(PG_FUNCTION_ARGS);

Datum quantile_numeric_array(PG_FUNCTION_ARGS);
Datum quantile_numeric(PG_FUNCTION_ARGS);

/* These functions use a bit dirty trick to pass the data - the int
 * value is actually a pointer to the array allocated in the parent
 * memory context. A bit ugly but works fine.
 *
 * The memory consumption might be a problem, as all the values are
 * kept in the memory - for example 1.000.000 of 8-byte values (bigint)
 * requires about 8MB of memory.
 */

Datum
quantile_append_double(PG_FUNCTION_ARGS)
{
    struct_double * data;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    /* OK, we do want to skip NULL values altogether */
    if (PG_ARGISNULL(1))
    {
        if (PG_ARGISNULL(0))
            PG_RETURN_NULL();
        else
            /* if there already is a state accumulated, don't forget it */
            PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }

    GET_AGG_CONTEXT("quantile_append_double", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0))
    {
        data = (struct_double*)palloc(sizeof(struct_double));
        data->elements  = (double*)palloc(4*sizeof(double));    /* 4*8B = 32B */
        data->nelements = 4;
        data->next = 0;
        data->sorted = false;

        data->quantiles = (double*)palloc(sizeof(double));
        data->quantiles[0] = PG_GETARG_FLOAT8(2);
        data->nquantiles = 1;

        check_quantiles(data->nquantiles, data->quantiles);
    }
    else
        data = (struct_double*)PG_GETARG_POINTER(0);

    /* we can be sure the value is not null (see the check above) */
    if (data->next > data->nelements-1)
    {
        data->nelements *= 2;
        data->elements = (double*)repalloc(data->elements, sizeof(double)*data->nelements);
    }

    data->elements[data->next++] = PG_GETARG_FLOAT8(1);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(data);
}

Datum
quantile_append_double_array(PG_FUNCTION_ARGS)
{
    struct_double * data;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    /* OK, we do want to skip NULL values altogether */
    if (PG_ARGISNULL(1))
    {
        if (PG_ARGISNULL(0))
            PG_RETURN_NULL();
        else
            /* if there already is a state accumulated, don't forget it */
            PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }

    GET_AGG_CONTEXT("quantile_append_double_array", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0))
    {
        data = (struct_double*)palloc(sizeof(struct_double));
        data->elements  = (double*)palloc(4*sizeof(double));
        data->nelements = 4;
        data->next = 0;
        data->sorted = false;

        /* read the array of quantiles */
        data->quantiles = array_to_double(fcinfo, PG_GETARG_ARRAYTYPE_P(2), &data->nquantiles);

        check_quantiles(data->nquantiles, data->quantiles);
    }
    else
        data = (struct_double*)PG_GETARG_POINTER(0);

    /* we can be sure the value is not null (see the check above) */
    if (data->next > data->nelements-1)
    {
        data->nelements *= 2;
        data->elements = (double*)repalloc(data->elements, sizeof(double)*data->nelements);
    }

    data->elements[data->next++] = PG_GETARG_FLOAT8(1);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(data);
}

Datum
quantile_append_numeric(PG_FUNCTION_ARGS)
{
    int len;
    Numeric element;
    struct_numeric * data;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    /* OK, we do want to skip NULL values altogether */
    if (PG_ARGISNULL(1))
    {
        if (PG_ARGISNULL(0))
            PG_RETURN_NULL();
        else
            /* if there already is a state accumulated, don't forget it */
            PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }

    GET_AGG_CONTEXT("quantile_append_numeric", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0))
    {
        data = (struct_numeric*)palloc(sizeof(struct_numeric));
        data->data = NULL;
        data->next = 0;
        data->usedlen = 0;
        data->maxlen = 32;    /* TODO make this a constant */

        data->quantiles = (double*)palloc(sizeof(double));
        data->quantiles[0] = PG_GETARG_FLOAT8(2);
        data->nquantiles = 1;

        check_quantiles(data->nquantiles, data->quantiles);
    }
    else
        data = (struct_numeric*)PG_GETARG_POINTER(0);

    element = PG_GETARG_NUMERIC(1);
    len = VARSIZE(element);

    /* we can be sure the value is not null (see the check above) */
    if (data->usedlen + len > data->maxlen)
    {
        while (len + data->usedlen > data->maxlen)
            data->maxlen *= 2;

        if (data->data != NULL)
            data->data = repalloc(data->data, data->maxlen);
    }

    /* if first entry, we need to allocate the buffer */
    if (! data->data)
        data->data = palloc(data->maxlen);

    /* copy the contents of the Numeric in place */
    memcpy(data->data + data->usedlen, element, len);

    data->usedlen += len;
    data->next += 1;

    MemoryContextSwitchTo(oldcontext);

    Assert(data->usedlen <= data->maxlen);
    Assert((!data->usedlen && !data->data) || (data->usedlen && data->data));

    PG_RETURN_POINTER(data);
}

Datum
quantile_append_numeric_array(PG_FUNCTION_ARGS)
{
    int len;
    Numeric element;
    struct_numeric * data;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    /* OK, we do want to skip NULL values altogether */
    if (PG_ARGISNULL(1))
    {
        if (PG_ARGISNULL(0))
            PG_RETURN_NULL();
        else
            /* if there already is a state accumulated, don't forget it */
            PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }

    GET_AGG_CONTEXT("quantile_append_numeric", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0))
    {
        data = (struct_numeric*)palloc(sizeof(struct_numeric));
        data->data = NULL;
        data->next = 0;
        data->usedlen = 0;
        data->maxlen = 32;    /* TODO make this a constant */

        /* read the array of quantiles */
        data->quantiles = array_to_double(fcinfo, PG_GETARG_ARRAYTYPE_P(2), &data->nquantiles);

        check_quantiles(data->nquantiles, data->quantiles);
    }
    else
        data = (struct_numeric*)PG_GETARG_POINTER(0);

    element = PG_GETARG_NUMERIC(1);
    len = VARSIZE(element);

    /* we can be sure the value is not null (see the check above) */
    if (data->usedlen + len > data->maxlen)
    {
        while (len + data->usedlen > data->maxlen)
            data->maxlen *= 2;

        if (data->data != NULL)
            data->data = repalloc(data->data, data->maxlen);
    }

    /* if first entry, we need to allocate the buffer */
    if (! data->data)
        data->data = palloc(data->maxlen);

    /* copy the contents of the Numeric in place */
    memcpy(data->data + data->usedlen, element, len);

    data->usedlen += len;
    data->next += 1;

    MemoryContextSwitchTo(oldcontext);

    Assert(data->usedlen <= data->maxlen);
    Assert((!data->usedlen && !data->data) || (data->usedlen && data->data));

    PG_RETURN_POINTER(data);
}

Datum
quantile_append_int32(PG_FUNCTION_ARGS)
{
    struct_int32 * data;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    /* OK, we do want to skip NULL values altogether */
    if (PG_ARGISNULL(1))
    {
        if (PG_ARGISNULL(0))
            PG_RETURN_NULL();
        else
            /* if there already is a state accumulated, don't forget it */
            PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }

    GET_AGG_CONTEXT("quantile_append_int32", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0))
    {
        data = (struct_int32*)palloc(sizeof(struct_int32));
        data->quantiles = (double*)palloc(sizeof(double));
        data->elements  = (int32*)palloc(8*sizeof(int32));
        data->nelements = 8;
        data->nquantiles = 1;
        data->quantiles[0] = PG_GETARG_FLOAT8(2);
        data->next = 0;
        data->sorted = false;

        check_quantiles(data->nquantiles, data->quantiles);
    }
    else
        data = (struct_int32*)PG_GETARG_POINTER(0);

    /* we can be sure the value is not null (see the check above) */
    if (data->next > data->nelements-1)
    {
        data->nelements *= 2;
        data->elements = (int32*)repalloc(data->elements, sizeof(int32)*data->nelements);
    }

    data->elements[data->next++] = PG_GETARG_INT32(1);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(data);
}

Datum
quantile_append_int32_array(PG_FUNCTION_ARGS)
{
    struct_int32 * data;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    /* OK, we do want to skip NULL values altogether */
    if (PG_ARGISNULL(1))
    {
        if (PG_ARGISNULL(0))
            PG_RETURN_NULL();
        else
            /* if there already is a state accumulated, don't forget it */
            PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }

    GET_AGG_CONTEXT("quantile_append_int32_array", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0))
    {
        data = (struct_int32*)palloc(sizeof(struct_int32));
        data->elements  = (int32*)palloc(4*sizeof(int32));
        data->nelements = 4;
        data->next = 0;
        data->sorted = false;

        /* read the array of quantiles */
        data->quantiles = array_to_double(fcinfo, PG_GETARG_ARRAYTYPE_P(2), &data->nquantiles);

        check_quantiles(data->nquantiles, data->quantiles);
    }
    else
        data = (struct_int32*)PG_GETARG_POINTER(0);

    /* we can be sure the value is not null (see the check above) */
    if (data->next > data->nelements-1)
    {
        data->nelements *= 2;
        data->elements = (int32*)repalloc(data->elements, sizeof(int32)*data->nelements);
    }

    data->elements[data->next++] = PG_GETARG_INT32(1);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(data);
}

Datum
quantile_append_int64(PG_FUNCTION_ARGS)
{

    struct_int64 * data;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    /* OK, we do want to skip NULL values altogether */
    if (PG_ARGISNULL(1))
    {
        if (PG_ARGISNULL(0))
            PG_RETURN_NULL();
        else
            /* if there already is a state accumulated, don't forget it */
            PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }

    GET_AGG_CONTEXT("quantile_append_int64", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0))
    {
        data = (struct_int64*)palloc(sizeof(struct_int64));
        data->quantiles = (double*)palloc(sizeof(double));
        data->elements  = (int64*)palloc(4*sizeof(int64));
        data->nelements = 4;
        data->nquantiles = 1;
        data->quantiles[0] = PG_GETARG_FLOAT8(2);
        data->next = 0;
        data->sorted = false;

        check_quantiles(data->nquantiles, data->quantiles);
    }
    else
        data = (struct_int64*)PG_GETARG_POINTER(0);

    /* we can be sure the value is not null (see the check above) */
    if (data->next > data->nelements-1)
    {
        data->nelements *= 2;
        data->elements = (int64*)repalloc(data->elements, sizeof(int64)*data->nelements);
    }

    data->elements[data->next++] = PG_GETARG_INT64(1);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(data);
}

Datum
quantile_append_int64_array(PG_FUNCTION_ARGS)
{
    struct_int64 * data;

    MemoryContext oldcontext;
    MemoryContext aggcontext;

    /* OK, we do want to skip NULL values altogether */
    if (PG_ARGISNULL(1))
    {
        if (PG_ARGISNULL(0))
            PG_RETURN_NULL();
        else
            /* if there already is a state accumulated, don't forget it */
            PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }

    GET_AGG_CONTEXT("quantile_append_int64_array", fcinfo, aggcontext);

    oldcontext = MemoryContextSwitchTo(aggcontext);

    if (PG_ARGISNULL(0))
    {
        data = (struct_int64*)palloc(sizeof(struct_int64));
        data->elements  = (int64*)palloc(4*sizeof(int64));
        data->nelements = 4;
        data->next = 0;
        data->sorted = false;

        /* read the array of quantiles */
        data->quantiles = array_to_double(fcinfo, PG_GETARG_ARRAYTYPE_P(2), &data->nquantiles);

        check_quantiles(data->nquantiles, data->quantiles);
    }
    else
        data = (struct_int64*)PG_GETARG_POINTER(0);

    /* we can be sure the value is not null (see the check above) */
    if (data->next > data->nelements-1)
    {
        data->nelements *= 2;
        data->elements = (int64*)repalloc(data->elements, sizeof(int64)*data->nelements);
    }

    data->elements[data->next++] = PG_GETARG_INT64(1);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER(data);
}

Datum
quantile_double_serial(PG_FUNCTION_ARGS)
{
    struct_double   *state = (struct_double *)PG_GETARG_POINTER(0);
    Size            hlen = offsetof(struct_double, quantiles);        /* header */
    Size            qlen = state->nquantiles * sizeof(double);        /* quantiles */
    Size            dlen = state->next * sizeof(double);            /* elements */
    bytea           *out = (bytea *)palloc(VARHDRSZ + qlen + dlen + hlen);
    char           *ptr;

    CHECK_AGG_CONTEXT("quantile_double_serial", fcinfo);

    /* we want to serialize the data in sorted format */
    sort_state_double(state);

    SET_VARSIZE(out, VARHDRSZ + qlen + dlen + hlen);
    ptr = VARDATA(out);

    memcpy(ptr, state, hlen);
    ptr += hlen;

    memcpy(ptr, state->quantiles, qlen);
    ptr += qlen;

    memcpy(ptr, state->elements, dlen);
    ptr += dlen;

    Assert(ptr == (char*)out + VARHDRSZ + hlen + qlen + dlen);

    PG_RETURN_BYTEA_P(out);
}

Datum
quantile_double_deserial(PG_FUNCTION_ARGS)
{
    struct_double *out = (struct_double *)palloc(sizeof(struct_double));
    bytea  *state = (bytea *)PG_GETARG_POINTER(0);
    Size    len = VARSIZE_ANY_EXHDR(state);
    char   *ptr = VARDATA(state);

    CHECK_AGG_CONTEXT("quantile_double_deserial", fcinfo);

    Assert(len > 0);

    /* copy the header */
    memcpy(out, ptr, offsetof(struct_double, quantiles));
    ptr += offsetof(struct_double, quantiles);

    Assert(out->nquantiles > 0);
    Assert((out->next > 0) && (out->nelements >= out->next));
    Assert(len == offsetof(struct_double, quantiles) +
                  (out->nquantiles * sizeof(double)) +
                  (out->next * sizeof(double)));
    Assert(out->sorted);

    /* we only allocate the necessary space */
    out->quantiles = (double *)palloc(out->nquantiles * sizeof(double));
    out->elements = (double *)palloc(out->next * sizeof(double));

    out->nelements = out->next;

    memcpy((void *)out->quantiles, ptr, out->nquantiles * sizeof(double));
    ptr += out->nquantiles * sizeof(double);

    memcpy((void *)out->elements, ptr, out->next * sizeof(double));
    ptr += out->next * sizeof(double);

    /* make sure we've consumed all serialized data */
    Assert(ptr == (char*)state + VARSIZE(state));

    PG_RETURN_POINTER(out);
}

Datum
quantile_double_combine(PG_FUNCTION_ARGS)
{
	int i, j, k;
	double *tmp;
	struct_double *state1;
	struct_double *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_double", fcinfo, agg_context);

	state1 = PG_ARGISNULL(0) ? NULL : (struct_double *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (struct_double *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = (struct_double *)palloc(sizeof(struct_double));
		state1->nelements = state2->nelements;
		state1->next = state2->next;

		state1->sorted = state2->sorted;

		state1->quantiles = (double*)palloc(sizeof(double) * state2->nquantiles);
		state1->elements = (double*)palloc(sizeof(double) * state2->nelements);

		memcpy(state1->quantiles, state2->quantiles, sizeof(double) * state2->nquantiles);
		memcpy(state1->elements, state2->elements, sizeof(double) * state2->nelements);

		MemoryContextSwitchTo(old_context);

		/* free the internal state */
		pfree(state2->elements);
		pfree(state2->quantiles);
		state2->elements = NULL;
		state2->quantiles = NULL;

		PG_RETURN_POINTER(state1);
	}

	Assert((state1 != NULL) && (state2 != NULL));

	/* make sure both states are sorted */
	sort_state_double(state1);
	sort_state_double(state2);

	tmp = (double*)MemoryContextAlloc(agg_context,
					  sizeof(double) * (state1->next + state2->next));

	/* merge the two arrays */
	i = j = k = 0;
	while (true)
	{
		Assert(k <= (state1->next + state2->next));
		Assert((i <= state1->next) && (j <= state2->next));

		if ((i < state1->next) && (j < state2->next))
		{
			if (state1->elements[i] <= state2->elements[j])
				tmp[k++] = state1->elements[i++];
			else
				tmp[k++] = state2->elements[j++];
		}
		else if (i < state1->nelements)
			tmp[k++] = state1->elements[i++];
		else if (j < state2->nelements)
			tmp[k++] = state2->elements[j++];
		else
			/* no more elements to process */
			break;
	}

	Assert(k == (state1->next + state2->next));
	Assert((i == state1->next) && (j == state2->next));

	/* free the two arrays */
	pfree(state1->elements);
	pfree(state2->elements);

	state1->elements = tmp;

	/* and finally remember the current number of elements */
	state1->next += state2->next;
	state1->nelements = state1->next;

	PG_RETURN_POINTER(state1);
}

Datum
quantile_int32_serial(PG_FUNCTION_ARGS)
{
    struct_int32   *state = (struct_int32 *)PG_GETARG_POINTER(0);
    Size            hlen = offsetof(struct_int32, quantiles);        /* header */
    Size            qlen = state->nquantiles * sizeof(double);       /* quantiles */
    Size            dlen = state->next * sizeof(int32);              /* elements */
    bytea           *out = (bytea *)palloc(VARHDRSZ + qlen + dlen + hlen);
    char           *ptr;

    CHECK_AGG_CONTEXT("quantile_int32_serial", fcinfo);

    /* we want to serialize the data in sorted format */
    sort_state_int32(state);

    SET_VARSIZE(out, VARHDRSZ + qlen + dlen + hlen);
    ptr = VARDATA(out);

    memcpy(ptr, state, hlen);
    ptr += hlen;

    memcpy(ptr, state->quantiles, qlen);
    ptr += qlen;

    memcpy(ptr, state->elements, dlen);
    ptr += dlen;

    Assert(ptr == (char*)out + VARHDRSZ + hlen + qlen + dlen);

    PG_RETURN_BYTEA_P(out);
}

Datum
quantile_int32_deserial(PG_FUNCTION_ARGS)
{
    struct_int32 *out = (struct_int32 *)palloc(sizeof(struct_int32));
    bytea  *state = (bytea *)PG_GETARG_POINTER(0);
    Size    len = VARSIZE_ANY_EXHDR(state);
    char   *ptr = VARDATA(state);

    CHECK_AGG_CONTEXT("quantile_double_deserial", fcinfo);

    Assert(len > 0);

    /* copy the header */
    memcpy(out, ptr, offsetof(struct_int32, quantiles));
    ptr += offsetof(struct_int32, quantiles);

    Assert(out->nquantiles > 0);
    Assert((out->next > 0) && (out->nelements >= out->next));
    Assert(len == offsetof(struct_int32, quantiles) +
                  (out->nquantiles * sizeof(double)) +
                  (out->next * sizeof(int32)));
    Assert(out->sorted);

    /* we only allocate the necessary space */
    out->quantiles = (double *)palloc(out->nquantiles * sizeof(double));
    out->elements = (int32 *)palloc(out->next * sizeof(int32));

    out->nelements = out->next;

    memcpy((void *)out->quantiles, ptr, out->nquantiles * sizeof(double));
    ptr += out->nquantiles * sizeof(double);

    memcpy((void *)out->elements, ptr, out->next * sizeof(int32));
    ptr += out->next * sizeof(int32);

    /* make sure we've consumed all serialized data */
    Assert(ptr == (char*)state + VARSIZE(state));

    PG_RETURN_POINTER(out);
}

Datum
quantile_int32_combine(PG_FUNCTION_ARGS)
{
	int i, j, k;
	int32 *tmp;
	struct_int32 *state1;
	struct_int32 *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_double", fcinfo, agg_context);

	state1 = PG_ARGISNULL(0) ? NULL : (struct_int32 *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (struct_int32 *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = (struct_int32 *)palloc(sizeof(struct_int32));
		state1->nelements = state2->nelements;
		state1->next = state2->next;

		state1->sorted = state2->sorted;

		state1->quantiles = (double*)palloc(sizeof(double) * state2->nquantiles);
		state1->elements = (int32*)palloc(sizeof(int32) * state2->nelements);

		memcpy(state1->quantiles, state2->quantiles, sizeof(double) * state2->nquantiles);
		memcpy(state1->elements, state2->elements, sizeof(int32) * state2->nelements);

		MemoryContextSwitchTo(old_context);

		/* free the internal state */
		pfree(state2->elements);
		pfree(state2->quantiles);
		state2->elements = NULL;
		state2->quantiles = NULL;

		PG_RETURN_POINTER(state1);
	}

	Assert((state1 != NULL) && (state2 != NULL));

	/* make sure both states are sorted */
	sort_state_int32(state1);
	sort_state_int32(state2);

	tmp = (int32*)MemoryContextAlloc(agg_context,
					  sizeof(int32) * (state1->next + state2->next));

	/* merge the two arrays */
	i = j = k = 0;
	while (true)
	{
		Assert(k <= (state1->next + state2->next));
		Assert((i <= state1->next) && (j <= state2->next));

		if ((i < state1->next) && (j < state2->next))
		{
			if (state1->elements[i] <= state2->elements[j])
				tmp[k++] = state1->elements[i++];
			else
				tmp[k++] = state2->elements[j++];
		}
		else if (i < state1->nelements)
			tmp[k++] = state1->elements[i++];
		else if (j < state2->nelements)
			tmp[k++] = state2->elements[j++];
		else
			/* no more elements to process */
			break;
	}

	Assert(k == (state1->next + state2->next));
	Assert((i == state1->next) && (j == state2->next));

	/* free the two arrays */
	pfree(state1->elements);
	pfree(state2->elements);

	state1->elements = tmp;

	/* and finally remember the current number of elements */
	state1->next += state2->next;
	state1->nelements = state1->next;

	PG_RETURN_POINTER(state1);
}

Datum
quantile_int64_serial(PG_FUNCTION_ARGS)
{
    struct_int64   *state = (struct_int64 *)PG_GETARG_POINTER(0);
    Size            hlen = offsetof(struct_int64, quantiles);      /* header */
    Size            qlen = state->nquantiles * sizeof(double);     /* quantiles */
    Size            dlen = state->next * sizeof(int64);            /* elements */
    bytea           *out = (bytea *)palloc(VARHDRSZ + qlen + dlen + hlen);
    char           *ptr;

    CHECK_AGG_CONTEXT("quantile_int64_serial", fcinfo);

    /* we want to serialize the data in sorted format */
    sort_state_int64(state);

    SET_VARSIZE(out, VARHDRSZ + qlen + dlen + hlen);
    ptr = VARDATA(out);

    memcpy(ptr, state, hlen);
    ptr += hlen;

    memcpy(ptr, state->quantiles, qlen);
    ptr += qlen;

    memcpy(ptr, state->elements, dlen);
    ptr += dlen;

    Assert(ptr == (char*)out + VARHDRSZ + hlen + qlen + dlen);

    PG_RETURN_BYTEA_P(out);
}

Datum
quantile_int64_deserial(PG_FUNCTION_ARGS)
{
    struct_int64 *out = (struct_int64 *)palloc(sizeof(struct_int64));
    bytea  *state = (bytea *)PG_GETARG_POINTER(0);
    Size    len = VARSIZE_ANY_EXHDR(state);
    char   *ptr = VARDATA(state);

    CHECK_AGG_CONTEXT("quantile_double_deserial", fcinfo);

    Assert(len > 0);

    /* copy the header */
    memcpy(out, ptr, offsetof(struct_int64, quantiles));
    ptr += offsetof(struct_int64, quantiles);

    Assert(out->nquantiles > 0);
    Assert((out->next > 0) && (out->nelements >= out->next));
    Assert(len == offsetof(struct_int64, quantiles) +
                  (out->nquantiles * sizeof(double)) +
                  (out->next * sizeof(int64)));
    Assert(out->sorted);

    /* we only allocate the necessary space */
    out->quantiles = (double *)palloc(out->nquantiles * sizeof(double));
    out->elements = (int64 *)palloc(out->next * sizeof(int64));

    out->nelements = out->next;

    memcpy((void *)out->quantiles, ptr, out->nquantiles * sizeof(double));
    ptr += out->nquantiles * sizeof(double);

    memcpy((void *)out->elements, ptr, out->next * sizeof(int64));
    ptr += out->next * sizeof(int64);

    /* make sure we've consumed all serialized data */
    Assert(ptr == (char*)state + VARSIZE(state));

    PG_RETURN_POINTER(out);
}

Datum
quantile_int64_combine(PG_FUNCTION_ARGS)
{
	int i, j, k;
	int64 *tmp;
	struct_int64 *state1;
	struct_int64 *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	GET_AGG_CONTEXT("trimmed_combine_double", fcinfo, agg_context);

	state1 = PG_ARGISNULL(0) ? NULL : (struct_int64 *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (struct_int64 *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = (struct_int64 *)palloc(sizeof(struct_int64));
		state1->nelements = state2->nelements;
		state1->next = state2->next;

		state1->sorted = state2->sorted;

		state1->quantiles = (double*)palloc(sizeof(double) * state2->nquantiles);
		state1->elements = (int64*)palloc(sizeof(int64) * state2->nelements);

		memcpy(state1->quantiles, state2->quantiles, sizeof(double) * state2->nquantiles);
		memcpy(state1->elements, state2->elements, sizeof(int64) * state2->nelements);

		MemoryContextSwitchTo(old_context);

		/* free the internal state */
		pfree(state2->elements);
		pfree(state2->quantiles);
		state2->elements = NULL;
		state2->quantiles = NULL;

		PG_RETURN_POINTER(state1);
	}

	Assert((state1 != NULL) && (state2 != NULL));

	/* make sure both states are sorted */
	sort_state_int64(state1);
	sort_state_int64(state2);

	tmp = (int64*)MemoryContextAlloc(agg_context,
					  sizeof(int64) * (state1->next + state2->next));

	/* merge the two arrays */
	i = j = k = 0;
	while (true)
	{
		Assert(k <= (state1->next + state2->next));
		Assert((i <= state1->next) && (j <= state2->next));

		if ((i < state1->next) && (j < state2->next))
		{
			if (state1->elements[i] <= state2->elements[j])
				tmp[k++] = state1->elements[i++];
			else
				tmp[k++] = state2->elements[j++];
		}
		else if (i < state1->nelements)
			tmp[k++] = state1->elements[i++];
		else if (j < state2->nelements)
			tmp[k++] = state2->elements[j++];
		else
			/* no more elements to process */
			break;
	}

	Assert(k == (state1->next + state2->next));
	Assert((i == state1->next) && (j == state2->next));

	/* free the two arrays */
	pfree(state1->elements);
	pfree(state2->elements);

	state1->elements = tmp;

	/* and finally remember the current number of elements */
	state1->next += state2->next;
	state1->nelements = state1->next;

	PG_RETURN_POINTER(state1);
}

Datum
quantile_double(PG_FUNCTION_ARGS)
{
    int     idx = 0;

    struct_double * data;

    CHECK_AGG_CONTEXT("quantile_double", fcinfo);

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    data = (struct_double*)PG_GETARG_POINTER(0);

    sort_state_double(data);

    if (data->quantiles[0] > 0)
        idx = (int)ceil(data->next * data->quantiles[0]) - 1;

    PG_RETURN_FLOAT8(data->elements[idx]);
}

Datum
quantile_double_array(PG_FUNCTION_ARGS)
{
    int i, idx;
    double * result;

    struct_double * data;

    CHECK_AGG_CONTEXT("quantile_double_array", fcinfo);

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    data = (struct_double*)PG_GETARG_POINTER(0);

    result = palloc(data->nquantiles * sizeof(double));

    sort_state_double(data);

    for (i = 0; i < data->nquantiles; i++)
    {
        idx = 0;
        if (data->quantiles[i] > 0)
            idx = (int)ceil(data->next * data->quantiles[i]) - 1;

        result[i] = data->elements[idx];
    }

    return double_to_array(fcinfo, result, data->nquantiles);
}

Datum
quantile_int32(PG_FUNCTION_ARGS)
{
    int idx = 0;
    struct_int32 * data;

    CHECK_AGG_CONTEXT("quantile_int32", fcinfo);

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    data = (struct_int32*)PG_GETARG_POINTER(0);

    sort_state_int32(data);

    if (data->quantiles[0] > 0)
        idx = (int)ceil(data->next * data->quantiles[0]) - 1;

    PG_RETURN_INT32(data->elements[idx]);
}

Datum
quantile_int32_array(PG_FUNCTION_ARGS)
{
    int i, idx;
    struct_int32 * data;
    int32 * result;

    CHECK_AGG_CONTEXT("quantile_int32_array", fcinfo);

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    data = (struct_int32*)PG_GETARG_POINTER(0);

    result = palloc(data->nquantiles * sizeof(int32));

    sort_state_int32(data);

    for (i = 0; i < data->nquantiles; i++)
    {
        idx = 0;
        if (data->quantiles[i] > 0)
            idx = (int)ceil(data->next * data->quantiles[i]) - 1;

        result[i] = data->elements[idx];
    }

    return int32_to_array(fcinfo, result, data->nquantiles);
}

Datum
quantile_int64(PG_FUNCTION_ARGS)
{
    int idx = 0;
    struct_int64 * data;

    CHECK_AGG_CONTEXT("quantile_int64", fcinfo);

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    data = (struct_int64*)PG_GETARG_POINTER(0);

    sort_state_int64(data);

    if (data->quantiles[0] > 0)
        idx = (int)ceil(data->next * data->quantiles[0]) - 1;

    PG_RETURN_INT64(data->elements[idx]);
}

Datum
quantile_int64_array(PG_FUNCTION_ARGS)
{
    int i, idx;
    struct_int64 * data;
    int64 * result;

    CHECK_AGG_CONTEXT("quantile_int64_array", fcinfo);

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    data = (struct_int64*)PG_GETARG_POINTER(0);

    result = palloc(data->nquantiles * sizeof(int64));

    sort_state_int64(data);

    for (i = 0; i < data->nquantiles; i++)
    {
        idx = 0;
        if (data->quantiles[i] > 0)
            idx = (int)ceil(data->next * data->quantiles[i]) - 1;

        result[i] = data->elements[idx];
    }

    return int64_to_array(fcinfo, result, data->nquantiles);
}

Datum
quantile_numeric(PG_FUNCTION_ARGS)
{
    int idx = 0;
    char *ptr;
    struct_numeric * data;

    CHECK_AGG_CONTEXT("quantile_numeric", fcinfo);

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    data = (struct_numeric*)PG_GETARG_POINTER(0);

    sort_state_numeric(data);

    if (data->quantiles[0] > 0)
        idx = (int)ceil(data->next * data->quantiles[0]) - 1;

    /* walk the array until you find the entry */
    ptr = data->data;
    while (idx > 0)
    {
        ptr += VARSIZE(ptr);
        idx--;
    }

    PG_RETURN_NUMERIC((Numeric)ptr);
}

Datum
quantile_numeric_array(PG_FUNCTION_ARGS)
{
    int i, idx;
    struct_numeric * data;
    Numeric * result;
    Numeric * elements;
    char *ptr;

    CHECK_AGG_CONTEXT("quantile_numeric_array", fcinfo);

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    data = (struct_numeric*)PG_GETARG_POINTER(0);

    result = palloc(data->nquantiles * sizeof(Numeric));

    sort_state_numeric(data);

    elements = palloc(data->next * sizeof(Numeric));

    idx = 0;
    ptr = data->data;
    while (ptr < data->data + data->usedlen)
    {
        Assert(idx < data->next);
        elements[idx++] = (Numeric)ptr;
        ptr += VARSIZE(ptr);
    }

    Assert(ptr == data->data + data->usedlen);
    Assert(idx == data->next);

    for (i = 0; i < data->nquantiles; i++)
    {
        idx = 0;
        if (data->quantiles[i] > 0)
            idx = (int)ceil(data->next * data->quantiles[i]) - 1;

        result[i] = elements[idx];
    }

    pfree(elements);

    return numeric_to_array(fcinfo, result, data->nquantiles);
}

/* Comparators for the qsort() calls. */

static int
double_comparator(const void *a, const void *b)
{
    double af = (*(double*)a);
    double bf = (*(double*)b);
    return (af > bf) - (af < bf);
}

static int
int32_comparator(const void *a, const void *b)
{
    int32 af = (*(int32*)a);
    int32 bf = (*(int32*)b);
    return (af > bf) - (af < bf);
}

static int
int64_comparator(const void *a, const void *b)
{
    int64 af = (*(int64*)a);
    int64 bf = (*(int64*)b);
    return (af > bf) - (af < bf);
}

static int
numeric_comparator(const void *a, const void *b)
{
    FunctionCallInfoData fcinfo;

    /* set params */
    fcinfo.arg[0] = NumericGetDatum(*(Numeric*)a);
    fcinfo.arg[1] = NumericGetDatum(*(Numeric*)b);
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;

    /* return the result */
    return DatumGetInt32(numeric_cmp(&fcinfo));
}

/*
 * Reading quantiles from an input array, based mostly on
 * array_to_text_internal (it's a modified copy). This expects
 * to receive a single-dimensional float8 array as input, fails
 * otherwise.
 */
static double *
array_to_double(FunctionCallInfo fcinfo, ArrayType *v, int * len)
{
    double     *result;
    int         nitems,
               *dims,
                ndims;
    Oid         element_type;
    int         typlen;
    bool        typbyval;
    char        typalign;
    char       *p;
    int         i, idx = 0;

    ArrayMetaState *my_extra;

    ndims = ARR_NDIM(v);
    dims = ARR_DIMS(v);
    nitems = ArrayGetNItems(ndims, dims);

    /* this is a special-purpose function for single-dimensional arrays */
    if (ndims != 1)
        elog(ERROR, "error, array_to_double expects a single-dimensional array"
                    "(dims = %d)", ndims);

    /* if there are no elements, set the length to 0 and return NULL */
    if (nitems == 0)
    {
        (*len) = 0;
        return NULL;
    }

    element_type = ARR_ELEMTYPE(v);

    result = (double*)palloc(nitems * sizeof(double));

    /*
     * We arrange to look up info about element type, including its output
     * conversion proc, only once per series of calls, assuming the element
     * type doesn't change underneath us.
     */
    my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL)
    {
        fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
                                                      sizeof(ArrayMetaState));
        my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
        my_extra->element_type = ~element_type;
    }

    if (my_extra->element_type != element_type)
    {
        /*
         * Get info about element type, including its output conversion proc
         */
        get_type_io_data(element_type, IOFunc_output,
                         &my_extra->typlen, &my_extra->typbyval,
                         &my_extra->typalign, &my_extra->typdelim,
                         &my_extra->typioparam, &my_extra->typiofunc);
        fmgr_info_cxt(my_extra->typiofunc, &my_extra->proc,
                      fcinfo->flinfo->fn_mcxt);
        my_extra->element_type = element_type;
    }

    typlen = my_extra->typlen;
    typbyval = my_extra->typbyval;
    typalign = my_extra->typalign;

    p = ARR_DATA_PTR(v);

    for (i = 0; i < nitems; i++)
    {
        Datum itemvalue = fetch_att(p, typbyval, typlen);

        double val = DatumGetFloat8(itemvalue);

        result[idx++] = val;

        p = att_addlength_pointer(p, typlen, p);
        p = (char *) att_align_nominal(p, typalign);
    }

    (*len) = idx;

    return result;
}

/*
 * Helper functions used to prepare the resulting array (when there's
 * an array of quantiles).
 */
static Datum
double_to_array(FunctionCallInfo fcinfo, double * d, int len)
{
    ArrayBuildState *astate = NULL;
    int         i;

    for (i = 0; i < len; i++)
    {
        /* stash away this field */
        astate = accumArrayResult(astate,
                                  Float8GetDatum(d[i]),
                                  false,
                                  FLOAT8OID,
                                  CurrentMemoryContext);
    }

    PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate,
                                          CurrentMemoryContext));
}

static Datum
int32_to_array(FunctionCallInfo fcinfo, int32 * d, int len)
{
    ArrayBuildState *astate = NULL;
    int         i;

    for (i = 0; i < len; i++)
    {
        /* stash away this field */
        astate = accumArrayResult(astate,
                                  Int32GetDatum(d[i]),
                                  false,
                                  INT4OID,
                                  CurrentMemoryContext);
    }

    PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate,
                                          CurrentMemoryContext));
}

static Datum
int64_to_array(FunctionCallInfo fcinfo, int64 * d, int len)
{

    ArrayBuildState *astate = NULL;
    int         i;

    for (i = 0; i < len; i++)
    {
        /* stash away this field */
        astate = accumArrayResult(astate,
                                  Int64GetDatum(d[i]),
                                  false,
                                  INT8OID,
                                  CurrentMemoryContext);
    }

    PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate,
                                          CurrentMemoryContext));
}

static Datum
numeric_to_array(FunctionCallInfo fcinfo, Numeric * d, int len)
{
    ArrayBuildState *astate = NULL;
    int         i;

    for (i = 0; i < len; i++)
    {
        /* stash away this field */
        astate = accumArrayResult(astate,
                                  NumericGetDatum(d[i]),
                                  false,
                                  NUMERICOID,
                                  CurrentMemoryContext);
    }

    PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate,
                                          CurrentMemoryContext));
}

static void
check_quantiles(int nquantiles, double * quantiles)
{
    int i = 0;
    for (i = 0; i < nquantiles; i++)
        if (quantiles[i] < 0 || quantiles[i] > 1)
            elog(ERROR, "invalid percentile value %f - needs to be in [0,1]", quantiles[i]);
}

static void
sort_state_double(struct_double *state)
{
    if (state->sorted)
        return;

    pg_qsort(state->elements, state->next, sizeof(double), &double_comparator);
    state->sorted = true;
}

static void
sort_state_int32(struct_int32 *state)
{
    if (state->sorted)
        return;

    pg_qsort(state->elements, state->next, sizeof(int32), &int32_comparator);
    state->sorted = true;
}

static void
sort_state_int64(struct_int64 *state)
{
    if (state->sorted)
        return;

    pg_qsort(state->elements, state->next, sizeof(int64), &int64_comparator);
    state->sorted = true;
}


static void
sort_state_numeric(struct_numeric *state)
{
    int        i;
    char   *data;
    char   *ptr;
    Numeric *items;

    /*
     * we'll sort a local copy of the data, and then copy it back (we want
     * to put the result into the proper memory context)
     */
    items = (Numeric*)palloc(sizeof(Numeric) * state->next);
    data = palloc(state->usedlen);
    memcpy(data, state->data, state->usedlen);

    /* parse the data into array of Numeric items, for pg_qsort */
    i = 0;
    ptr = data;
    while (ptr < data + state->usedlen)
    {
        items[i++] = (Numeric)ptr;
        ptr += VARSIZE(ptr);

        Assert(i <= state->next);
        Assert(ptr <= (data + state->usedlen));
    }

    Assert(i == state->next);
    Assert(ptr == (data + state->usedlen));

    pg_qsort(items, state->next, sizeof(Numeric), &numeric_comparator);

    /* copy the values from the local array back into the state */
    ptr = state->data;
    for (i = 0; i < state->next; i++)
    {
        memcpy(ptr, items[i], VARSIZE(items[i]));
        ptr += VARSIZE(items[i]);

        Assert(ptr <= state->data + state->usedlen);
    }

    Assert(ptr == state->data + state->usedlen);

    pfree(items);
    pfree(data);
}
