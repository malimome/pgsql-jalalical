/*-------------------------------------------------------------------------
 *
 * ptimestamp.h
 *	  Definitions for the "ptimestamp" and "interval" types.
 *
 *  By Mohsen Alimomeni
 *
 *-------------------------------------------------------------------------
 */
#ifndef PTIMESTAMP_H
#define PTIMESTAMP_H

#include <math.h>
#include <limits.h>
#include <float.h>

#include "fmgr.h"
#include "pgtime.h"
#ifdef HAVE_INT64_TIMESTAMP
#include "utils/int8.h"
#endif

/*
 * PTimestamp represents absolute time.
 * Interval represents delta time. Keep track of months (and years), days,
 *	and time separately since the elapsed time spanned is unknown until
 *	instantiated relative to an absolute time.
 *
 * Note that Postgres uses "time interval" to mean a bounded interval,
 * consisting of a beginning and ending time, not a time span - thomas 97/03/20
 */

//PG_MODULE_MAGIC;
#ifdef HAVE_INT64_TIMESTAMP
typedef int64 PTimestamp;
typedef int64 PTimestampTz;
#else
typedef double PTimestamp;
typedef double PTimestampTz;
#endif

#define MAX_TIMESTAMP_PRECISION 6
#define MAX_INTERVAL_PRECISION 6

/* in both ptimestamp.h and ecpg/dt.h */
#define DAYS_PER_YEAR	365.25	/* assumes leap year every four years */
#define MONTHS_PER_YEAR 12
/*
 *	DAYS_PER_MONTH is very imprecise.  The more accurate value is
 *	365.2425/12 = 30.436875, or '30 days 10:29:06'.  Right now we only
 *	return an integral number of days, but someday perhaps we should
 *	also return a 'time' value to be used as well.	ISO 8601 suggests
 *	30 days.
 */
#define DAYS_PER_MONTH	30		/* assumes exactly 30 days per month */
#define HOURS_PER_DAY	24		/* assume no daylight savings time changes */

/*
 *	This doesn't adjust for uneven daylight savings time intervals or leap
 *	seconds, and it crudely estimates leap years.  A more accurate value
 *	for days per years is 365.2422.
 */
#define SECS_PER_YEAR	(36525 * 864)	/* avoid floating-point computation */
#define SECS_PER_DAY	86400
#define SECS_PER_HOUR	3600
#define SECS_PER_MINUTE 60
#define MINS_PER_HOUR	60

#ifdef HAVE_INT64_TIMESTAMP
#define USECS_PER_DAY	INT64CONST(86400000000)
#define USECS_PER_HOUR	INT64CONST(3600000000)
#define USECS_PER_MINUTE INT64CONST(60000000)
#define USECS_PER_SEC	INT64CONST(1000000)
#endif

/*
 * Macros for fmgr-callable functions.
 *
 * For PTimestamp, we make use of the same support routines as for int64
 * or float8.  Therefore PTimestamp is pass-by-reference if and only if
 * int64 or float8 is!
 */
#define PG_GETARG_PTIMESTAMP(n) (PTimestamp *)PG_GETARG_POINTER(n)
#define PG_RETURN_PTIMESTAMP(x) PG_RETURN_POINTER(x)
#define PG_GETARG_PTIMESTAMPTZ(n) (PTimestampTz *)PG_GETARG_POINTER(n)
#define PG_RETURN_PTIMESTAMPTZ(x) PG_RETURN_POINTER(x)


#ifdef HAVE_INT64_TIMESTAMP

//#define DatumGetPTimestamp(X)  ((PTimestamp) DatumGetInt64(X))
//#define DatumGetPTimestampTz(X)	((PTimestampTz) DatumGetInt64(X))

#define PTimestampGetDatum(X) Int64GetDatum(X)
//#define PTimestampTzGetDatum(X) Int64GetDatum(X)

//#define PG_GETARG_PTIMESTAMP(n) PG_GETARG_INT64(n)
//#define PG_GETARG_PTIMESTAMPTZ(n) PG_GETARG_INT64(n)

//#define PG_RETURN_PTIMESTAMP(x) PG_RETURN_INT64(x)
//#define PG_RETURN_PTIMESTAMPTZ(x) PG_RETURN_INT64(x)

#define DT_NOBEGIN		(-INT64CONST(0x7fffffffffffffff) - 1)
#define DT_NOEND		(INT64CONST(0x7fffffffffffffff))
#else

//#define DatumGetPTimestamp(X)  ((PTimestamp) DatumGetFloat8(X))
//#define DatumGetPTimestampTz(X)	((PTimestampTz) DatumGetFloat8(X))

#define PTimestampGetDatum(X) Float8GetDatum(X)
//#define PTimestampTzGetDatum(X) Float8GetDatum(X)

//#define PG_GETARG_PTIMESTAMP(n) DatumGetPTimestamp(PG_GETARG_DATUM(n))
//#define PG_GETARG_PTIMESTAMPTZ(n) DatumGetPTimestampTz(PG_GETARG_DATUM(n))

//#define PG_RETURN_PTIMESTAMP(x) return PTimestampGetDatum(x)
//#define PG_RETURN_PTIMESTAMPTZ(x) return PTimestampTzGetDatum(x)

#ifdef HUGE_VAL
#define DT_NOBEGIN		(-HUGE_VAL)
#define DT_NOEND		(HUGE_VAL)
#else
#define DT_NOBEGIN		(-DBL_MAX)
#define DT_NOEND		(DBL_MAX)
#endif
#endif   /* HAVE_INT64_TIMESTAMP */


#define PTIMESTAMP_NOBEGIN(j)	\
	do {(j) = DT_NOBEGIN;} while (0)
#define PTIMESTAMP_IS_NOBEGIN(j) ((j) == DT_NOBEGIN)

#define PTIMESTAMP_NOEND(j)		\
	do {(j) = DT_NOEND;} while (0)
#define PTIMESTAMP_IS_NOEND(j)	((j) == DT_NOEND)

#define PTIMESTAMP_NOT_FINITE(j) (PTIMESTAMP_IS_NOBEGIN(j) || PTIMESTAMP_IS_NOEND(j))
/*
#ifdef HAVE_INT64_TIMESTAMP

typedef int32 fsec_t;
#else

typedef double fsec_t;
#endif
*/

/*
 *	Round off to MAX_PTIMESTAMP_PRECISION decimal places.
 *	Note: this is also used for rounding off intervals.
 */
#define TS_PREC_INV 1000000.0
#define TSROUND(j) (rint(((double) (j)) * TS_PREC_INV) / TS_PREC_INV)

#define TIMESTAMP_MASK(b) (1 << (b))
#define INTERVAL_MASK(b) (1 << (b))

/* Macros to handle packing and unpacking the typmod field for intervals */
#define INTERVAL_FULL_RANGE (0x7FFF)
#define INTERVAL_RANGE_MASK (0x7FFF)
#define INTERVAL_FULL_PRECISION (0xFFFF)
#define INTERVAL_PRECISION_MASK (0xFFFF)
#define INTERVAL_TYPMOD(p,r) ((((r) & INTERVAL_RANGE_MASK) << 16) | ((p) & INTERVAL_PRECISION_MASK))
#define INTERVAL_PRECISION(t) ((t) & INTERVAL_PRECISION_MASK)
#define INTERVAL_RANGE(t) (((t) >> 16) & INTERVAL_RANGE_MASK)

#ifdef HAVE_INT64_TIMESTAMP
#define PTimestampTzPlusMilliseconds(tz,ms) ((tz) + ((ms) * (int64) 1000))
#else
#define PTimestampTzPlusMilliseconds(tz,ms) ((tz) + ((ms) / 1000.0))
#endif


/* Set at postmaster start */
PTimestampTz PgStartTime_P;


/*
 * ptimestamp.c prototypes
 */

Datum ptimestamp_in(PG_FUNCTION_ARGS);
Datum ptimestamp_out(PG_FUNCTION_ARGS);
Datum ptimestamp_recv(PG_FUNCTION_ARGS);
Datum ptimestamp_send(PG_FUNCTION_ARGS);
Datum ptimestamptypmodin(PG_FUNCTION_ARGS);
Datum ptimestamptypmodout(PG_FUNCTION_ARGS);
Datum ptimestamp_scale(PG_FUNCTION_ARGS);
Datum ptimestamp_eq(PG_FUNCTION_ARGS);
Datum ptimestamp_ne(PG_FUNCTION_ARGS);
Datum ptimestamp_lt(PG_FUNCTION_ARGS);
Datum ptimestamp_le(PG_FUNCTION_ARGS);
Datum ptimestamp_ge(PG_FUNCTION_ARGS);
Datum ptimestamp_gt(PG_FUNCTION_ARGS);
Datum ptimestamp_finite(PG_FUNCTION_ARGS);
Datum ptimestamp_cmp(PG_FUNCTION_ARGS);
Datum ptimestamp_hash(PG_FUNCTION_ARGS);
Datum ptimestamp_smaller(PG_FUNCTION_ARGS);
Datum ptimestamp_larger(PG_FUNCTION_ARGS);

Datum ptimestamp_eq_ptimestamptz(PG_FUNCTION_ARGS);
Datum ptimestamp_ne_ptimestamptz(PG_FUNCTION_ARGS);
Datum ptimestamp_lt_ptimestamptz(PG_FUNCTION_ARGS);
Datum ptimestamp_le_ptimestamptz(PG_FUNCTION_ARGS);
Datum ptimestamp_gt_ptimestamptz(PG_FUNCTION_ARGS);
Datum ptimestamp_ge_ptimestamptz(PG_FUNCTION_ARGS);
Datum ptimestamp_cmp_ptimestamptz(PG_FUNCTION_ARGS);

Datum ptimestamptz_eq_ptimestamp(PG_FUNCTION_ARGS);
Datum ptimestamptz_ne_ptimestamp(PG_FUNCTION_ARGS);
Datum ptimestamptz_lt_ptimestamp(PG_FUNCTION_ARGS);
Datum ptimestamptz_le_ptimestamp(PG_FUNCTION_ARGS);
Datum ptimestamptz_gt_ptimestamp(PG_FUNCTION_ARGS);
Datum ptimestamptz_ge_ptimestamp(PG_FUNCTION_ARGS);
Datum ptimestamptz_cmp_ptimestamp(PG_FUNCTION_ARGS);

Datum ptimestamp_trunc(PG_FUNCTION_ARGS);
Datum ptimestamp_part(PG_FUNCTION_ARGS);
Datum ptimestamp_zone(PG_FUNCTION_ARGS);
Datum ptimestamp_izone(PG_FUNCTION_ARGS);
Datum ptimestamp_ptimestamptz(PG_FUNCTION_ARGS);

Datum ptimestamptz_in(PG_FUNCTION_ARGS);
Datum ptimestamptz_out(PG_FUNCTION_ARGS);
Datum ptimestamptz_recv(PG_FUNCTION_ARGS);
Datum ptimestamptz_send(PG_FUNCTION_ARGS);
Datum ptimestamptztypmodin(PG_FUNCTION_ARGS);
Datum ptimestamptztypmodout(PG_FUNCTION_ARGS);
Datum ptimestamptz_scale(PG_FUNCTION_ARGS);
Datum ptimestamptz_ptimestamp(PG_FUNCTION_ARGS);
Datum ptimestamptz_zone(PG_FUNCTION_ARGS);
Datum ptimestamptz_izone(PG_FUNCTION_ARGS);
Datum ptimestamptz_ptimestamptz(PG_FUNCTION_ARGS);
Datum timestamp_ptimestamp(PG_FUNCTION_ARGS);
Datum ptimestamp_timestamp(PG_FUNCTION_ARGS);

Datum interval_um(PG_FUNCTION_ARGS);
Datum interval_pl(PG_FUNCTION_ARGS);
Datum interval_mi(PG_FUNCTION_ARGS);
Datum interval_mul(PG_FUNCTION_ARGS);
Datum mul_d_interval(PG_FUNCTION_ARGS);
Datum interval_div(PG_FUNCTION_ARGS);
Datum interval_accum(PG_FUNCTION_ARGS);
Datum interval_avg(PG_FUNCTION_ARGS);

Datum ptimestamp_mi(PG_FUNCTION_ARGS);
Datum ptimestamp_pl_interval(PG_FUNCTION_ARGS);
Datum ptimestamp_mi_interval(PG_FUNCTION_ARGS);
Datum ptimestamp_age(PG_FUNCTION_ARGS);
Datum overlaps_ptimestamp(PG_FUNCTION_ARGS);

Datum ptimestamptz_pl_interval(PG_FUNCTION_ARGS);
Datum ptimestamptz_mi_interval(PG_FUNCTION_ARGS);
Datum ptimestamptz_age(PG_FUNCTION_ARGS);
Datum ptimestamptz_trunc(PG_FUNCTION_ARGS);
Datum ptimestamptz_part(PG_FUNCTION_ARGS);
/*
Datum now(PG_FUNCTION_ARGS);
Datum statement_ptimestamp(PG_FUNCTION_ARGS);
Datum clock_ptimestamp(PG_FUNCTION_ARGS);

Datum pgsql_postmaster_start_time(PG_FUNCTION_ARGS);
*/
/* Internal routines (not fmgr-callable) */

PTimestampTz GetCurrentPTimestamp(void);

void PTimestampDifference(PTimestampTz start_time, PTimestampTz stop_time,
					long *secs, int *microsecs);
bool PTimestampDifferenceExceeds(PTimestampTz start_time,
						   PTimestampTz stop_time,
						   int msec);

PTimestampTz time_t_to_ptimestamptz(time_t tm);
time_t ptimestamptz_to_time_t(PTimestampTz t);

const char *ptimestamptz_to_str(PTimestampTz t);

int	tm2ptimestamp(struct pg_tm * tm, fsec_t fsec, int *tzp, PTimestamp *dt);
int ptimestamp2tm(PTimestamp dt, int *tzp, struct pg_tm * tm,
			 fsec_t *fsec, char **tzn, pg_tz *attimezone);
void dt2time(PTimestamp dt, int *hour, int *min, int *sec, fsec_t *fsec);

//int	interval2tm(Interval span, struct pg_tm * tm, fsec_t *fsec);
//int	tm2interval(struct pg_tm * tm, fsec_t fsec, Interval *span);

PTimestamp SetEpochPTimestamp(void);
void GetEpochTime(struct pg_tm * tm);

int	ptimestamp_cmp_internal(PTimestamp *dt1, PTimestamp *dt2);

/* ptimestamp comparison works for ptimestamptz also */
#define ptimestamptz_cmp_internal(dt1,dt2)	ptimestamp_cmp_internal(dt1, dt2)

int	isoweek2j(int year, int week);
void isoweek2date(int woy, int *year, int *mon, int *mday);
void isoweekdate2date(int isoweek, int isowday, int *year, int *mon, int *mday);
int	date2isoweek(int year, int mon, int mday);
int	date2isoyear(int year, int mon, int mday);
int	date2isoyearday(int year, int mon, int mday);

#endif   /* PTIMESTAMP_H */
