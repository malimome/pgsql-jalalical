/*-------------------------------------------------------------------------
 *
 * pdate.c
 *	  implements PDATE: Persian Date according to Iranian Calendar. 
 *	
 *	Implementation by: Mohsen Alimomeni
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <limits.h>
#include <float.h>
#include <time.h>

#include "access/hash.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "pdate.h"
#include "pdatetime.h"
#include "ptimestamp.h"

#ifdef __FAST_MATH__
#error -ffast-math is known to break this code
#endif

/* pdate_in()
 * Given pdate text string, convert to internal date format.
 */
PG_FUNCTION_INFO_V1(pdate_in);
Datum
pdate_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	PDATE		date;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			tzp;
	int			dtype;
	int			nf;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[MAXDATELEN + 1];

	dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
						  field, ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = P_DecodeDateTime(field, ftype, nf, &dtype, tm, &fsec, &tzp);
	if (dterr != 0)
		DateTimeParseError(dterr, str, "date");

	switch (dtype)
	{
		case DTK_DATE:
			break;

		case DTK_CURRENT:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			  errmsg("date/time value \"current\" is no longer supported")));

			P_GetCurrentDateTime(tm);
			break;

		case DTK_EPOCH:
			GetEpochTime(tm);
			break;

		default:
			DateTimeParseError(DTERR_BAD_FORMAT, str, "date");
			break;
	}
	date = date2p(tm->tm_year, tm->tm_mon, tm->tm_mday);

	PG_RETURN_PDATE(date);
}

/* pdate_out()
 * Given internal format date, convert to text string.
 */
PG_FUNCTION_INFO_V1(pdate_out);
Datum
pdate_out(PG_FUNCTION_ARGS)
{
	PDATE date = PG_GETARG_PDATE(0);
	char	   *result;
	struct pg_tm tt,
			   *tm = &tt;
	char		buf[MAXDATELEN + 1];

	p2date(date ,
		   &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));

	EncodeDateOnly(tm, DateStyle, buf);

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/*
 *		pdate_recv			- converts external binary format to date
 */
PG_FUNCTION_INFO_V1(pdate_recv);
Datum
pdate_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_PDATE((PDATE) pq_getmsgint(buf, sizeof(PDATE)));
}

/*
 *		pdate_send			- converts date to binary format
 */
PG_FUNCTION_INFO_V1(pdate_send);
Datum
pdate_send(PG_FUNCTION_ARGS)
{
	PDATE		date = PG_GETARG_PDATE(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint(&buf, date, sizeof(date));
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/*
 * Comparison functions for dates
 */

PG_FUNCTION_INFO_V1(pdate_eq);
Datum
pdate_eq(PG_FUNCTION_ARGS)
{
	PDATE		dateVal1 = PG_GETARG_PDATE(0);
	PDATE		dateVal2 = PG_GETARG_PDATE(1);

	PG_RETURN_BOOL(dateVal1 == dateVal2);
}

PG_FUNCTION_INFO_V1(pdate_ne);
Datum
pdate_ne(PG_FUNCTION_ARGS)
{
	PDATE		dateVal1 = PG_GETARG_PDATE(0);
	PDATE		dateVal2 = PG_GETARG_PDATE(1);

	PG_RETURN_BOOL(dateVal1 != dateVal2);
}

PG_FUNCTION_INFO_V1(pdate_lt);
Datum
pdate_lt(PG_FUNCTION_ARGS)
{
	PDATE		dateVal1 = PG_GETARG_PDATE(0);
	PDATE		dateVal2 = PG_GETARG_PDATE(1);

	PG_RETURN_BOOL(dateVal1 < dateVal2);
}

PG_FUNCTION_INFO_V1(pdate_le);
Datum
pdate_le(PG_FUNCTION_ARGS)
{
	PDATE		dateVal1 = PG_GETARG_PDATE(0);
	PDATE		dateVal2 = PG_GETARG_PDATE(1);

	PG_RETURN_BOOL(dateVal1 <= dateVal2);
}

PG_FUNCTION_INFO_V1(pdate_gt);
Datum
pdate_gt(PG_FUNCTION_ARGS)
{
	PDATE		dateVal1 = PG_GETARG_PDATE(0);
	PDATE		dateVal2 = PG_GETARG_PDATE(1);

	PG_RETURN_BOOL(dateVal1 > dateVal2);
}

PG_FUNCTION_INFO_V1(pdate_ge);
Datum
pdate_ge(PG_FUNCTION_ARGS)
{
	PDATE		dateVal1 = PG_GETARG_PDATE(0);
	PDATE		dateVal2 = PG_GETARG_PDATE(1);

	PG_RETURN_BOOL(dateVal1 >= dateVal2);
}

PG_FUNCTION_INFO_V1(pdate_cmp);
Datum
pdate_cmp(PG_FUNCTION_ARGS)
{
	PDATE		dateVal1 = PG_GETARG_PDATE(0);
	PDATE		dateVal2 = PG_GETARG_PDATE(1);

	if (dateVal1 < dateVal2)
		PG_RETURN_INT32(-1);
	else if (dateVal1 > dateVal2)
		PG_RETURN_INT32(1);
	PG_RETURN_INT32(0);
}

PG_FUNCTION_INFO_V1(pdate_larger);
Datum
pdate_larger(PG_FUNCTION_ARGS)
{
	PDATE		dateVal1 = PG_GETARG_PDATE(0);
	PDATE		dateVal2 = PG_GETARG_PDATE(1);

	PG_RETURN_PDATE((dateVal1 > dateVal2) ? dateVal1 : dateVal2);
}

PG_FUNCTION_INFO_V1(pdate_smaller);
Datum
pdate_smaller(PG_FUNCTION_ARGS)
{
	PDATE		dateVal1 = PG_GETARG_PDATE(0);
	PDATE		dateVal2 = PG_GETARG_PDATE(1);

	PG_RETURN_PDATE((dateVal1 < dateVal2) ? dateVal1 : dateVal2);
}

/* Compute difference between two dates in days.
 */
PG_FUNCTION_INFO_V1(pdate_mi);
Datum
pdate_mi(PG_FUNCTION_ARGS)
{
	PDATE		dateVal1 = PG_GETARG_PDATE(0);
	PDATE		dateVal2 = PG_GETARG_PDATE(1);

	PG_RETURN_INT32((int32) (dateVal1 - dateVal2));
}

/* Add a number of days to a date, giving a new date.
 * Must handle both positive and negative numbers of days.
 */
PG_FUNCTION_INFO_V1(pdate_pli);
Datum
pdate_pli(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	int32		days = PG_GETARG_INT32(1);

	PG_RETURN_PDATE(dateVal + days);
}

/* Subtract a number of days from a date, giving a new date.
 */
PG_FUNCTION_INFO_V1(pdate_mii);
Datum
pdate_mii(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	int32		days = PG_GETARG_INT32(1);

	PG_RETURN_PDATE(dateVal - days);
}

/*
 * Internal routines for promoting date to timestamp and timestamp with
 * time zone
 */
static PTimestamp
pdate2ptimestamp(PDATE dateVal)
{
	PTimestamp	result;

#ifdef HAVE_INT64_TIMESTAMP
	// date is days since 2000, timestamp is microseconds since same... 
	result = dateVal * USECS_PER_DAY;
	// Date's range is wider than timestamp's, so must check for overflow 
	if (result / USECS_PER_DAY != dateVal)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("date out of range for timestamp")));
#else
	// date is days since 2000, timestamp is seconds since same... 
	result = dateVal * (double) SECS_PER_DAY;
#endif

	return result;
}

// Converts a pdate to ptimestamptz
static PTimestampTz
pdate2ptimestamptz(PDATE dateVal)
{
	PTimestampTz result;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;

	p2date(dateVal, &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));

	tm->tm_hour = 0;
	tm->tm_min = 0;
	tm->tm_sec = 0;
	tz = P_DetermineTimeZoneOffset(tm, session_timezone);

#ifdef HAVE_INT64_TIMESTAMP
	result = dateVal * USECS_PER_DAY + tz * USECS_PER_SEC;
	// Date's range is wider than timestamp's, so must check for overflow 
	if ((result - tz * USECS_PER_SEC) / USECS_PER_DAY != dateVal)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("date out of range for timestamp")));
#else
	result = dateVal * (double) SECS_PER_DAY + tz;
#endif

	return result;
}

/*
 * Crosstype comparison functions for dates
 */
PG_FUNCTION_INFO_V1(pdate_eq_ptimestamp);
Datum
pdate_eq_ptimestamp(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);
	PTimestamp	dt1;

	dt1 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) == 0);
}

PG_FUNCTION_INFO_V1(pdate_ne_ptimestamp);
Datum
pdate_ne_ptimestamp(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);
	PTimestamp	dt1;

	dt1 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) != 0);
}

PG_FUNCTION_INFO_V1(pdate_lt_ptimestamp);
Datum
pdate_lt_ptimestamp(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);
	PTimestamp	dt1;

	dt1 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) < 0);
}

PG_FUNCTION_INFO_V1(pdate_gt_ptimestamp);
Datum
pdate_gt_ptimestamp(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);
	PTimestamp	dt1;

	dt1 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) > 0);
}

PG_FUNCTION_INFO_V1(pdate_le_ptimestamp);
Datum
pdate_le_ptimestamp(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);
	PTimestamp	dt1;

	dt1 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) <= 0);
}

PG_FUNCTION_INFO_V1(pdate_ge_ptimestamp);
Datum
pdate_ge_ptimestamp(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);
	PTimestamp	dt1;

	dt1 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) >= 0);
}

PG_FUNCTION_INFO_V1(pdate_cmp_ptimestamp);
Datum
pdate_cmp_ptimestamp(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);
	PTimestamp	dt1;

	dt1 = pdate2ptimestamp(dateVal);

	PG_RETURN_INT32(ptimestamp_cmp_internal(&dt1, dt2));
}

PG_FUNCTION_INFO_V1(pdate_eq_ptimestamptz);
Datum
pdate_eq_ptimestamptz(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(&dt1, dt2) == 0);
}

PG_FUNCTION_INFO_V1(pdate_ne_ptimestamptz);
Datum
pdate_ne_ptimestamptz(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(&dt1, dt2) != 0);
}

PG_FUNCTION_INFO_V1(pdate_lt_ptimestamptz);
Datum
pdate_lt_ptimestamptz(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(&dt1, dt2) < 0);
}

PG_FUNCTION_INFO_V1(pdate_gt_ptimestamptz);
Datum
pdate_gt_ptimestamptz(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(&dt1, dt2) > 0);
}

PG_FUNCTION_INFO_V1(pdate_le_ptimestamptz);
Datum
pdate_le_ptimestamptz(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(&dt1, dt2) <= 0);
}

PG_FUNCTION_INFO_V1(pdate_ge_ptimestamptz);
Datum
pdate_ge_ptimestamptz(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(&dt1, dt2) >= 0);
}

PG_FUNCTION_INFO_V1(pdate_cmp_ptimestamptz);
Datum
pdate_cmp_ptimestamptz(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = pdate2ptimestamptz(dateVal);

	PG_RETURN_INT32(ptimestamptz_cmp_internal(&dt1, dt2));
}

PG_FUNCTION_INFO_V1(ptimestamp_eq_pdate);
Datum
ptimestamp_eq_pdate(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestamp	dt2;

	dt2 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) == 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_ne_pdate);
Datum
ptimestamp_ne_pdate(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestamp	dt2;

	dt2 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) != 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_lt_pdate);
Datum
ptimestamp_lt_pdate(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestamp	dt2;

	dt2 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) < 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_gt_pdate);
Datum
ptimestamp_gt_pdate(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestamp	dt2;

	dt2 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) > 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_le_pdate);
Datum
ptimestamp_le_pdate(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestamp	dt2;

	dt2 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) <= 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_ge_pdate);
Datum
ptimestamp_ge_pdate(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestamp	dt2;

	dt2 = pdate2ptimestamp(dateVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) >= 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_cmp_pdate);
Datum
ptimestamp_cmp_pdate(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestamp	dt2;

	dt2 = pdate2ptimestamp(dateVal);

	PG_RETURN_INT32(ptimestamp_cmp_internal(dt1, &dt2));
}

PG_FUNCTION_INFO_V1(ptimestamptz_eq_pdate);
Datum
ptimestamptz_eq_pdate(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestampTz dt2;

	dt2 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(dt1, &dt2) == 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_ne_pdate);
Datum
ptimestamptz_ne_pdate(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestampTz dt2;

	dt2 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(dt1, &dt2) != 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_lt_pdate);
Datum
ptimestamptz_lt_pdate(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestampTz dt2;

	dt2 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(dt1, &dt2) < 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_gt_pdate);
Datum
ptimestamptz_gt_pdate(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestampTz dt2;

	dt2 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(dt1, &dt2) > 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_le_pdate);
Datum
ptimestamptz_le_pdate(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestampTz dt2;

	dt2 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(dt1, &dt2) <= 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_ge_pdate);
Datum
ptimestamptz_ge_pdate(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestampTz dt2;

	dt2 = pdate2ptimestamptz(dateVal);

	PG_RETURN_BOOL(ptimestamptz_cmp_internal(dt1, &dt2) >= 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_cmp_pdate);
Datum
ptimestamptz_cmp_pdate(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PDATE		dateVal = PG_GETARG_PDATE(1);
	PTimestampTz dt2;

	dt2 = pdate2ptimestamptz(dateVal);

	PG_RETURN_INT32(ptimestamptz_cmp_internal(dt1, &dt2));
}

/* Add an interval to a date, giving a new date.
 * Must handle both positive and negative intervals.
 *
 * We implement this by promoting the date to timestamp (without time zone)
 * and then using the timestamp plus interval function.
 */
PG_FUNCTION_INFO_V1(pdate_pl_interval);
Datum
pdate_pl_interval(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	PTimestamp	dateStamp;

	dateStamp = pdate2ptimestamp(dateVal);

	return DirectFunctionCall2(ptimestamp_pl_interval,
							   PTimestampGetDatum(dateStamp),
							   PointerGetDatum(span));
}
/* Subtract an interval from a date, giving a new date.
 * Must handle both positive and negative intervals.
 *
 * We implement this by promoting the date to timestamp (without time zone)
 * and then using the timestamp minus interval function.
 */
PG_FUNCTION_INFO_V1(pdate_mi_interval);
Datum
pdate_mi_interval(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	PTimestamp	dateStamp;

	dateStamp = pdate2ptimestamp(dateVal);

	return DirectFunctionCall2(ptimestamp_mi_interval,
							   PTimestampGetDatum(dateStamp),
							   PointerGetDatum(span));
}
/* pdate_date()
 * Convert pdate to date data type.
 */
PG_FUNCTION_INFO_V1(pdate_date);
Datum
pdate_date(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	DateADT		result;
	
	struct pg_tm ptm, tm;	
	p2date(dateVal, &ptm.tm_year, &ptm.tm_mon, &ptm.tm_mday);
	pdate2date(ptm, &tm);
	
	result = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

	PG_RETURN_DATEADT(result);
}

/* date_pdate()
 * Convert date to pdate data type.
 */
PG_FUNCTION_INFO_V1(date_pdate);
Datum
date_pdate(PG_FUNCTION_ARGS)
{
	DateADT	dateVal = PG_GETARG_DATEADT(0);
	PDATE		result;
	
	struct pg_tm ptm, tm;	
	j2date(dateVal + POSTGRES_EPOCH_JDATE , 
				&tm.tm_year, &tm.tm_mon, &tm.tm_mday);
	date2pdate(tm, &ptm);
	
	result = date2p(ptm.tm_year, ptm.tm_mon, ptm.tm_mday); 

	PG_RETURN_PDATE(result);
}
/* pdate_ptimestamp()
 * Convert date to timestamp data type.
 */
PG_FUNCTION_INFO_V1(pdate_ptimestamp);
Datum
pdate_ptimestamp(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestamp	*result;

	result = (PTimestamp *) palloc(sizeof(PTimestamp));
	*result = pdate2ptimestamp(dateVal);

	PG_RETURN_PTIMESTAMP(result);
}

/* ptimestamp_pdate()
 * Convert timestamp to date data type.
 */
PG_FUNCTION_INFO_V1(ptimestamp_pdate);
Datum
ptimestamp_pdate(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(0);
	PDATE		result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;

	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_NULL();

	if (timestamp2tm(*timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = date2p(tm->tm_year, tm->tm_mon, tm->tm_mday);

	PG_RETURN_PDATE(result);
}

/* pdate_ptimestamptz()
 * Convert date to timestamp with time zone data type.
 */
PG_FUNCTION_INFO_V1(pdate_ptimestamptz);
Datum
pdate_ptimestamptz(PG_FUNCTION_ARGS)
{
	PDATE		dateVal = PG_GETARG_PDATE(0);
	PTimestampTz result;

	result = pdate2ptimestamptz(dateVal);

	PG_RETURN_PTIMESTAMP(result);
}

/* ptimestamptz_pdate()
 * Convert timestamp with time zone to date data type.
 */
PG_FUNCTION_INFO_V1(ptimestamptz_pdate);
Datum
ptimestamptz_pdate(PG_FUNCTION_ARGS)
{
	PTimestampTz *timestamp = PG_GETARG_PTIMESTAMP(0);
	PDATE		result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	int			tz;
	char	   *tzn;

	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_NULL();

	if (timestamp2tm(*timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = date2p(tm->tm_year, tm->tm_mon, tm->tm_mday);

	PG_RETURN_PDATE(result);
}

/* abstime_pdate()
 * Convert abstime to date data type.
 */
/*
Datum
abstime_pdate(PG_FUNCTION_ARGS)
{
	AbsoluteTime abstime = PG_GETARG_ABSOLUTETIME(0);
	PDATE		result;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;

	switch (abstime)
	{
		case INVALID_ABSTIME:
		case NOSTART_ABSTIME:
		case NOEND_ABSTIME:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				   errmsg("cannot convert reserved abstime value to date")));

			
			// * pretend to drop through to make compiler think that result will
			// * be set
			

		default:
			abstime2tm(abstime, &tz, tm, NULL);
			result = date2p(tm->tm_year, tm->tm_mon, tm->tm_mday); 
			break;
	}

	PG_RETURN_PDATE(result);
}
*/
/* AdjustTimeForTypmod()
 * Force the precision of the time value to a specified value.
 * Uses *exactly* the same code as in AdjustPTimestampForTypemod()
 * but we make a separate copy because those types do not
 * have a fundamental tie together but rather a coincidence of
 * implementation. - thomas
 */
/*
static void
AdjustTimeForTypmod(TimeADT *time, int32 typmod)
{
#ifdef HAVE_INT64_TIMESTAMP
	static const int64 TimeScales[MAX_TIME_PRECISION + 1] = {
		INT64CONST(1000000),
		INT64CONST(100000),
		INT64CONST(10000),
		INT64CONST(1000),
		INT64CONST(100),
		INT64CONST(10),
		INT64CONST(1)
	};

	static const int64 TimeOffsets[MAX_TIME_PRECISION + 1] = {
		INT64CONST(500000),
		INT64CONST(50000),
		INT64CONST(5000),
		INT64CONST(500),
		INT64CONST(50),
		INT64CONST(5),
		INT64CONST(0)
	};
#else
	// note MAX_TIME_PRECISION differs in this case 
	static const double TimeScales[MAX_TIME_PRECISION + 1] = {
		1.0,
		10.0,
		100.0,
		1000.0,
		10000.0,
		100000.0,
		1000000.0,
		10000000.0,
		100000000.0,
		1000000000.0,
		10000000000.0
	};
#endif

	if (typmod >= 0 && typmod <= MAX_TIME_PRECISION)
	{
		
		// * Note: this round-to-nearest code is not completely consistent about
		// * rounding values that are exactly halfway between integral values.
		// * On most platforms, rint() will implement round-to-nearest-even, but
		// * the integer code always rounds up (away from zero).	Is it worth
		// * trying to be consistent?
		 
#ifdef HAVE_INT64_TIMESTAMP
		if (*time >= INT64CONST(0))
			*time = ((*time + TimeOffsets[typmod]) / TimeScales[typmod]) *
				TimeScales[typmod];
		else
			*time = -((((-*time) + TimeOffsets[typmod]) / TimeScales[typmod]) *
					  TimeScales[typmod]);
#else
		*time = rint((double) *time * TimeScales[typmod]) / TimeScales[typmod];
#endif
	}
}
*/

/* tm2time()
 * Convert a tm structure to a time data type.
 */
/*
static int
tm2time(struct pg_tm * tm, fsec_t fsec, TimeADT *result)
{
#ifdef HAVE_INT64_TIMESTAMP
	*result = ((((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec)
			   * USECS_PER_SEC) + fsec;
#else
	*result = ((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec + fsec;
#endif
	return 0;
}
*/
/* time2tm()
 * Convert time data type to POSIX time structure.
 * For dates within the system-supported time_t range, convert to the
 *	local time zone. If out of this range, leave as GMT. - tgl 97/05/27
 */
/*
static int
time2tm(TimeADT time, struct pg_tm * tm, fsec_t *fsec)
{
#ifdef HAVE_INT64_TIMESTAMP
	tm->tm_hour = time / USECS_PER_HOUR;
	time -= tm->tm_hour * USECS_PER_HOUR;
	tm->tm_min = time / USECS_PER_MINUTE;
	time -= tm->tm_min * USECS_PER_MINUTE;
	tm->tm_sec = time / USECS_PER_SEC;
	time -= tm->tm_sec * USECS_PER_SEC;
	*fsec = time;
#else
	double		trem;

recalc:
	trem = time;
	TMODULO(trem, tm->tm_hour, (double) SECS_PER_HOUR);
	TMODULO(trem, tm->tm_min, (double) SECS_PER_MINUTE);
	TMODULO(trem, tm->tm_sec, 1.0);
	trem = TIMEROUND(trem);
//	 roundoff may need to propagate to higher-order fields 
	if (trem >= 1.0)
	{
		time = ceil(time);
		goto recalc;
	}
	*fsec = trem;
#endif

	return 0;
}
*/
/* ptimestamp_time()
 * Convert timestamp to time data type.
 */
PG_FUNCTION_INFO_V1(ptimestamp_time);
Datum
ptimestamp_time(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(0);
	TimeADT		result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;

	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_NULL();

	if (timestamp2tm(*timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP

	
	 // Could also do this with time = (timestamp / USECS_PER_DAY
	 // USECS_PER_DAY) - timestamp;
	 
	result = ((((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec) *
			  USECS_PER_SEC) + fsec;
#else
	result = ((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec + fsec;
#endif

	PG_RETURN_TIMEADT(result);
}
/* ptimestamptz_time()
 * Convert timestamptz to time data type.
 */
PG_FUNCTION_INFO_V1(ptimestamptz_time);
Datum
ptimestamptz_time(PG_FUNCTION_ARGS)
{
	PTimestampTz *timestamp = PG_GETARG_PTIMESTAMP(0);
	TimeADT		result;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;
	fsec_t		fsec;
	char	   *tzn;

	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_NULL();

	if (timestamp2tm(*timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP

	
	 // Could also do this with time = (timestamp / USECS_PER_DAY
	 // USECS_PER_DAY) - timestamp;
	 
	result = ((((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec) *
			  USECS_PER_SEC) + fsec;
#else
	result = ((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec + fsec;
#endif

	PG_RETURN_TIMEADT(result);
}
/* datetime_ptimestamp()
 * Convert date and time to timestamp data type.
 */
PG_FUNCTION_INFO_V1(pdatetime_ptimestamp);
Datum
pdatetime_ptimestamp(PG_FUNCTION_ARGS)
{
	PDATE		date = PG_GETARG_PDATE(0);
	TimeADT		time = PG_GETARG_TIMEADT(1);
	PTimestamp	result;

	result = (PTimestamp) DatumGetFloat8(DirectFunctionCall1(pdate_ptimestamp,
												   PDateGetDatum(date)));
	result += time;

	PG_RETURN_PTIMESTAMP(result);
}
/* tm2timetz()
 * Convert a tm structure to a time data type.
 */
static int
tm2timetz(struct pg_tm * tm, fsec_t fsec, int tz, TimeTzADT *result)
{
#ifdef HAVE_INT64_TIMESTAMP
	result->time = ((((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec) *
					USECS_PER_SEC) + fsec;
#else
	result->time = ((tm->tm_hour * MINS_PER_HOUR + tm->tm_min) * SECS_PER_MINUTE) + tm->tm_sec + fsec;
#endif
	result->zone = tz;

	return 0;
}

/* ptimestamptz_timetz()
 * Convert timestamp to timetz data type.
 */
PG_FUNCTION_INFO_V1(ptimestamptz_timetz);
Datum
ptimestamptz_timetz(PG_FUNCTION_ARGS)
{
	PTimestampTz *timestamp = PG_GETARG_PTIMESTAMP(0);
	TimeTzADT  *result;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;
	fsec_t		fsec;
	char	   *tzn;

	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_NULL();

	if (timestamp2tm(*timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

	tm2timetz(tm, fsec, tz, result);

	PG_RETURN_TIMETZADT_P(result);
}

/* datetimetz_ptimestamptz()
 * Convert date and ptimetz to timestamp with time zone data type.
 * PTimestamp is stored in GMT, so add the time zone
 * stored with the ptimetz to the result.
 * - thomas 2000-03-10
 */
PG_FUNCTION_INFO_V1(datetimetz_ptimestamptz);
Datum
datetimetz_ptimestamptz(PG_FUNCTION_ARGS)
{
	PDATE		date = PG_GETARG_PDATE(0);
	TimeTzADT  *time = PG_GETARG_TIMETZADT_P(1);
	PTimestampTz result;

#ifdef HAVE_INT64_TIMESTAMP
	result = date * USECS_PER_DAY + time->time + time->zone * USECS_PER_SEC;
#else
	result = date * (double) SECS_PER_DAY + time->time + time->zone;
#endif

	PG_RETURN_PTIMESTAMP(result);
}

