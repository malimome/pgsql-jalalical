/*-------------------------------------------------------------------------
 *
 * pdate.h
 *	  Definitions for the "pdate" type.
 *
 *
 *
 * By: Mohsen Alimomeni
 *
 *-------------------------------------------------------------------------
 */
#ifndef PDATE_H
#define PDATE_H

#include <math.h>

#include "fmgr.h"

typedef int32 PDATE;
typedef int32 DateADT;

#ifdef HAVE_INT64_TIMESTAMP
typedef int64 TimeADT;
#else
typedef float8 TimeADT;
#endif

typedef struct
{
#ifdef HAVE_INT64_TIMESTAMP
	int64		time;			/* all time units other than months and years */
#else
	double		time;			/* all time units other than months and years */
#endif
	int32		zone;			/* numeric time zone, in seconds */
} TimeTzADT;

/*
 * Macros for fmgr-callable functions.
 *
 * For TimeADT, we make use of the same support routines as for float8 or int64.
 * Therefore TimeADT is pass-by-reference if and only if float8 or int64 is!
 */
#ifdef HAVE_INT64_TIMESTAMP

#define MAX_TIME_PRECISION 6

#define DatumGetPDate(X)	  ((PDATE) DatumGetInt32(X))
#define DatumGetTimeADT(X)	  ((TimeADT) DatumGetInt64(X))
#define DatumGetTimeTzADTP(X) ((TimeTzADT *) DatumGetPointer(X))

#define PDateGetDatum(X)	  Int32GetDatum(X)
#define TimeADTGetDatum(X)	  Int64GetDatum(X)
#define TimeTzADTPGetDatum(X) PointerGetDatum(X)
#else

#define MAX_TIME_PRECISION 10

/* round off to MAX_TIME_PRECISION decimal places */
#define TIME_PREC_INV 10000000000.0
#define TIMEROUND(j) (rint(((double) (j)) * TIME_PREC_INV) / TIME_PREC_INV)

#define DatumGetPDate(X)	  ((PDATE) DatumGetInt32(X))
#define DatumGetTimeADT(X)	  ((TimeADT) DatumGetFloat8(X))
#define DatumGetTimeTzADTP(X) ((TimeTzADT *) DatumGetPointer(X))

#define PDateGetDatum(X)	  Int32GetDatum(X)
#define TimeADTGetDatum(X)	  Float8GetDatum(X)
#define TimeTzADTPGetDatum(X) PointerGetDatum(X)
#endif   /* HAVE_INT64_TIMESTAMP */

#define PG_GETARG_PDATE(n)	 DatumGetPDate(PG_GETARG_DATUM(n))
#define PG_GETARG_TIMEADT(n)	 DatumGetTimeADT(PG_GETARG_DATUM(n))
#define PG_GETARG_TIMETZADT_P(n) DatumGetTimeTzADTP(PG_GETARG_DATUM(n))

#define PG_RETURN_PDATE(x)	 return PDateGetDatum(x)
#define PG_RETURN_TIMEADT(x)	 return TimeADTGetDatum(x)
#define PG_RETURN_TIMETZADT_P(x) return TimeTzADTPGetDatum(x)


#define PG_RETURN_DATEADT(x)	 PG_RETURN_PDATE(x)	 
#define PG_GETARG_DATEADT(n)	 ((DateADT) DatumGetInt32(PG_GETARG_DATUM(n)))
/* date.c */
Datum pdate_in(PG_FUNCTION_ARGS);
Datum pdate_out(PG_FUNCTION_ARGS);
Datum pdate_recv(PG_FUNCTION_ARGS);
Datum pdate_send(PG_FUNCTION_ARGS);
Datum pdate_eq(PG_FUNCTION_ARGS);
Datum pdate_ne(PG_FUNCTION_ARGS);
Datum pdate_lt(PG_FUNCTION_ARGS);
Datum pdate_le(PG_FUNCTION_ARGS);
Datum pdate_gt(PG_FUNCTION_ARGS);
Datum pdate_ge(PG_FUNCTION_ARGS);
Datum pdate_cmp(PG_FUNCTION_ARGS);
Datum pdate_larger(PG_FUNCTION_ARGS);
Datum pdate_smaller(PG_FUNCTION_ARGS);
Datum pdate_mi(PG_FUNCTION_ARGS);
Datum pdate_pli(PG_FUNCTION_ARGS);
Datum pdate_mii(PG_FUNCTION_ARGS);
Datum pdate_eq_ptimestamp(PG_FUNCTION_ARGS);
Datum pdate_ne_ptimestamp(PG_FUNCTION_ARGS);
Datum pdate_lt_ptimestamp(PG_FUNCTION_ARGS);
Datum pdate_le_ptimestamp(PG_FUNCTION_ARGS);
Datum pdate_gt_ptimestamp(PG_FUNCTION_ARGS);
Datum pdate_ge_ptimestamp(PG_FUNCTION_ARGS);
Datum pdate_cmp_ptimestamp(PG_FUNCTION_ARGS);
Datum pdate_eq_ptimestamptz(PG_FUNCTION_ARGS);
Datum pdate_ne_ptimestamptz(PG_FUNCTION_ARGS);
Datum pdate_lt_ptimestamptz(PG_FUNCTION_ARGS);
Datum pdate_le_ptimestamptz(PG_FUNCTION_ARGS);
Datum pdate_gt_ptimestamptz(PG_FUNCTION_ARGS);
Datum pdate_ge_ptimestamptz(PG_FUNCTION_ARGS);
Datum pdate_cmp_ptimestamptz(PG_FUNCTION_ARGS);
Datum ptimestamp_eq_pdate(PG_FUNCTION_ARGS);
Datum ptimestamp_ne_pdate(PG_FUNCTION_ARGS);
Datum ptimestamp_lt_pdate(PG_FUNCTION_ARGS);
Datum ptimestamp_le_pdate(PG_FUNCTION_ARGS);
Datum ptimestamp_gt_pdate(PG_FUNCTION_ARGS);
Datum ptimestamp_ge_pdate(PG_FUNCTION_ARGS);
Datum ptimestamp_cmp_pdate(PG_FUNCTION_ARGS);
Datum ptimestamptz_eq_pdate(PG_FUNCTION_ARGS);
Datum ptimestamptz_ne_pdate(PG_FUNCTION_ARGS);
Datum ptimestamptz_lt_pdate(PG_FUNCTION_ARGS);
Datum ptimestamptz_le_pdate(PG_FUNCTION_ARGS);
Datum ptimestamptz_gt_pdate(PG_FUNCTION_ARGS);
Datum ptimestamptz_ge_pdate(PG_FUNCTION_ARGS);
Datum ptimestamptz_cmp_pdate(PG_FUNCTION_ARGS);
Datum pdate_pl_interval(PG_FUNCTION_ARGS);
Datum pdate_mi_interval(PG_FUNCTION_ARGS);
Datum pdate_ptimestamp(PG_FUNCTION_ARGS);
Datum pdatetime_ptimestamp(PG_FUNCTION_ARGS);
Datum ptimestamp_pdate(PG_FUNCTION_ARGS);
Datum pdate_ptimestamptz(PG_FUNCTION_ARGS);
Datum ptimestamptz_pdate(PG_FUNCTION_ARGS);
Datum dateptime_ptimestamp(PG_FUNCTION_ARGS);
Datum absptime_pdate(PG_FUNCTION_ARGS);

Datum timetypmodin(PG_FUNCTION_ARGS);
Datum timetypmodout(PG_FUNCTION_ARGS);
Datum ptimestamp_time(PG_FUNCTION_ARGS);
Datum ptimestamptz_time(PG_FUNCTION_ARGS);
Datum ptimestamptz_timetz(PG_FUNCTION_ARGS);
Datum datetimetz_ptimestamptz(PG_FUNCTION_ARGS);

Datum date_pdate(PG_FUNCTION_ARGS);
Datum pdate_date(PG_FUNCTION_ARGS);
#endif   /* PDATE_H */
