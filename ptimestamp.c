/*-------------------------------------------------------------------------
 *
 * ptimestamp.c
 *	  Functions for the add-in SQL92 types "ptimestamp" and "ptimestamptz".
 *
 *
 * IDENTIFICATION
 *	 Implemented by: Mohsen Alimomeni 
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <math.h>
#include <float.h>
#include <limits.h>
#include <sys/time.h>

#include "access/hash.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "pdatetime.h"
#include "ptimestamp.h"

/*
 * gcc's -ffast-math switch breaks routines that expect exact results from
 * expressions like timeval / SECS_PER_HOUR, where timeval is double.
 */
#ifdef __FAST_MATH__
#error -ffast-math is known to break this code
#endif

PG_MODULE_MAGIC;

/* Set at postmaster start */
PTimestampTz PgStartTime_P;

#ifdef HAVE_INT64_TIMESTAMP
static int64 time2t(const int hour, const int min, const int sec, const fsec_t fsec);
#else
static double time2t(const int hour, const int min, const int sec, const fsec_t fsec);
#endif
static int	EncodeSpecialPTimestamp(PTimestamp dt, char *str);
static PTimestamp dt2local(PTimestamp dt, int timezone);
static void AdjustPTimestampForTypmod(PTimestamp *time, int32 typmod);
static PTimestampTz ptimestamp2ptimestamptz(PTimestamp timestamp);


/* common code for timestamptypmodin and timestamptztypmodin */
static int32
anytimestamp_typmodin(bool istz, ArrayType *ta)
{
	int32		typmod;
	int32	   *tl;
	int			n;

	tl = ArrayGetIntegerTypmods(ta, &n);

	/*
	 * we're not too tense about good error message here because grammar
	 * shouldn't allow wrong number of modifiers for PTIMESTAMP
	 */
	if (n != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid type modifier")));

	if (*tl < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("PTIMESTAMP(%d)%s precision must not be negative",
						*tl, (istz ? " WITH TIME ZONE" : ""))));
	if (*tl > MAX_TIMESTAMP_PRECISION)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("PTIMESTAMP(%d)%s precision reduced to maximum allowed, %d",
				  *tl, (istz ? " WITH TIME ZONE" : ""),
				  MAX_TIMESTAMP_PRECISION)));
		typmod = MAX_TIMESTAMP_PRECISION;
	}
	else
		typmod = *tl;

	return typmod;
}

/* common code for timestamptypmodout and timestamptztypmodout */
static char *
anytimestamp_typmodout(bool istz, int32 typmod)
{
	char	   *res = (char *) palloc(64);
	const char *tz = istz ? " with time zone" : " without time zone";

	if (typmod >= 0)
		snprintf(res, 64, "(%d)%s", (int) typmod, tz);
	else
		snprintf(res, 64, "%s", tz);

	return res;
}


/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/

/* ptimestamp_in()
 * Convert a string to internal form.
 */
PG_FUNCTION_INFO_V1(ptimestamp_in);
Datum
ptimestamp_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	PTimestamp	*result;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;
	int			dtype;
	int			nf;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[MAXDATELEN + MAXDATEFIELDS];

	dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
						  field, ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = P_DecodeDateTime(field, ftype, nf, &dtype, tm, &fsec, &tz);
	if (dterr != 0)
		DateTimeParseError(dterr, str, "ptimestamp");

	
	result = (PTimestamp *) palloc(sizeof(PTimestamp));
	switch (dtype)
	{
		case DTK_DATE:
			if (tm2ptimestamp(tm, fsec, NULL, result) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("ptimestamp out of range: \"%s\"", str)));
			break;

		case DTK_EPOCH:
			*result = SetEpochPTimestamp();
			break;

		case DTK_LATE:
			PTIMESTAMP_NOEND(*result);
			break;

		case DTK_EARLY:
			PTIMESTAMP_NOBEGIN(*result);
			break;

		case DTK_INVALID:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			  errmsg("date/time value \"%s\" is no longer supported", str)));

			PTIMESTAMP_NOEND(*result);
			break;

		default:
			elog(ERROR, "unexpected dtype %d while parsing timestamp \"%s\"",
				 dtype, str);
			PTIMESTAMP_NOEND(*result);
	}

	AdjustPTimestampForTypmod(result, typmod);

	PG_RETURN_PTIMESTAMP(result);
}

/* ptimestamp_out()
 * Convert a timestamp to external form.
 */
PG_FUNCTION_INFO_V1(ptimestamp_out);
Datum
ptimestamp_out(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestamp = (PTimestamp *) PG_GETARG_PTIMESTAMP(0);
	char	   *result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	char	   *tzn = NULL;
	char		buf[MAXDATELEN + 1];

	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		EncodeSpecialPTimestamp(*timestamp, buf);
	else if (ptimestamp2tm(*timestamp, NULL, tm, &fsec, NULL, NULL) == 0)
		EncodeDateTime(tm, fsec, NULL, &tzn, DateStyle, buf);
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/*
 *		ptimestamp_recv			- converts external binary format to timestamp
 *
 * We make no attempt to provide compatibility between int and float
 * timestamp representations ...
 */
PG_FUNCTION_INFO_V1(ptimestamp_recv);
Datum
ptimestamp_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	PTimestamp *timestamp;
	timestamp = (PTimestamp *) palloc(sizeof(PTimestamp));

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;

#ifdef HAVE_INT64_TIMESTAMP
	*timestamp = (PTimestamp) pq_getmsgint64(buf);
#else
	*timestamp = (PTimestamp) pq_getmsgfloat8(buf);
#endif

	/* rangecheck: see if ptimestamp_out would like it */
	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		 /* ok */ ;
	else if (ptimestamp2tm(*timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	AdjustPTimestampForTypmod(timestamp, typmod);

	PG_RETURN_PTIMESTAMP(*timestamp);
}

/*
 *		ptimestamp_send			- converts timestamp to binary format
 */
PG_FUNCTION_INFO_V1(ptimestamp_send);
Datum
ptimestamp_send(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestamp = (PTimestamp *)	PG_GETARG_PTIMESTAMP(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
#ifdef HAVE_INT64_TIMESTAMP
	pq_sendint64(&buf, *timestamp);
#else
	pq_sendfloat8(&buf, *timestamp);
#endif
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

PG_FUNCTION_INFO_V1(ptimestamptypmodin);
Datum
ptimestamptypmodin(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);

	PG_RETURN_INT32(anytimestamp_typmodin(false, ta));
}

PG_FUNCTION_INFO_V1(ptimestamptypmodout);
Datum
ptimestamptypmodout(PG_FUNCTION_ARGS)
{
	int32		typmod = PG_GETARG_INT32(0);

	PG_RETURN_CSTRING(anytimestamp_typmodout(false, typmod));
}


/* ptimestamp_scale()
 * Adjust time type for specified scale factor.
 * Used by PostgreSQL type system to stuff columns.
 */
PG_FUNCTION_INFO_V1(ptimestamp_scale);
Datum
ptimestamp_scale(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(0);
	int32		typmod = PG_GETARG_INT32(1);
	PTimestamp	result;

	result = *timestamp;

	AdjustPTimestampForTypmod(&result, typmod);

	PG_RETURN_PTIMESTAMP(result);
}

static void
AdjustPTimestampForTypmod(PTimestamp *time, int32 typmod)
{
#ifdef HAVE_INT64_TIMESTAMP
	static const int64 PTimestampScales[MAX_TIMESTAMP_PRECISION + 1] = {
		INT64CONST(1000000),
		INT64CONST(100000),
		INT64CONST(10000),
		INT64CONST(1000),
		INT64CONST(100),
		INT64CONST(10),
		INT64CONST(1)
	};

	static const int64 PTimestampOffsets[MAX_TIMESTAMP_PRECISION + 1] = {
		INT64CONST(500000),
		INT64CONST(50000),
		INT64CONST(5000),
		INT64CONST(500),
		INT64CONST(50),
		INT64CONST(5),
		INT64CONST(0)
	};
#else
	static const double PTimestampScales[MAX_TIMESTAMP_PRECISION + 1] = {
		1,
		10,
		100,
		1000,
		10000,
		100000,
		1000000
	};
#endif

	if (!PTIMESTAMP_NOT_FINITE(*time)
		&& (typmod != -1) && (typmod != MAX_TIMESTAMP_PRECISION))
	{
		if (typmod < 0 || typmod > MAX_TIMESTAMP_PRECISION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				  errmsg("timestamp(%d) precision must be between %d and %d",
						 typmod, 0, MAX_TIMESTAMP_PRECISION)));

		/*
		 * Note: this round-to-nearest code is not completely consistent about
		 * rounding values that are exactly halfway between integral values.
		 * On most platforms, rint() will implement round-to-nearest-even, but
		 * the integer code always rounds up (away from zero).	Is it worth
		 * trying to be consistent?
		 */
#ifdef HAVE_INT64_TIMESTAMP
		if (*time >= INT64CONST(0))
		{
			*time = ((*time + PTimestampOffsets[typmod]) / PTimestampScales[typmod]) *
				PTimestampScales[typmod];
		}
		else
		{
			*time = -((((-*time) + PTimestampOffsets[typmod]) / PTimestampScales[typmod])
					  * PTimestampScales[typmod]);
		}
#else
		*time = rint((double) *time * PTimestampScales[typmod]) / PTimestampScales[typmod];
#endif
	}
}

/* ptimestamptz_in()
 * Convert a string to internal form.
 */

PG_FUNCTION_INFO_V1(ptimestamptz_in);
Datum
ptimestamptz_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	PTimestampTz *result;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;
	int			dtype;
	int			nf;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[MAXDATELEN + MAXDATEFIELDS];

	dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
						  field, ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = P_DecodeDateTime(field, ftype, nf, &dtype, tm, &fsec, &tz);
	if (dterr != 0)
		DateTimeParseError(dterr, str, "timestamp with time zone");

	result = (PTimestampTz *) palloc(sizeof(PTimestampTz));
	switch (dtype)
	{
		case DTK_DATE:
			if (tm2ptimestamp(tm, fsec, &tz, result) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range: \"%s\"", str)));
			break;

		case DTK_EPOCH:
			*result = SetEpochPTimestamp();
			break;

		case DTK_LATE:
			PTIMESTAMP_NOEND(*result);
			break;

		case DTK_EARLY:
			PTIMESTAMP_NOBEGIN(*result);
			break;

		case DTK_INVALID:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			  errmsg("date/time value \"%s\" is no longer supported", str)));

			PTIMESTAMP_NOEND(*result);
			break;

		default:
			elog(ERROR, "unexpected dtype %d while parsing timestamptz \"%s\"",
				 dtype, str);
			PTIMESTAMP_NOEND(*result);
	}

	AdjustPTimestampForTypmod(result, typmod);

	PG_RETURN_PTIMESTAMPTZ(result);
}

/* ptimestamptz_out()
 * Convert a timestamp to external form.
 */
PG_FUNCTION_INFO_V1(ptimestamptz_out);
Datum
ptimestamptz_out(PG_FUNCTION_ARGS)
{
	PTimestampTz	*timestamp = (PTimestampTz *) PG_GETARG_PTIMESTAMP(0);
	PTimestampTz dt = *timestamp;
	char	   *result;
	int			tz;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	char	   *tzn;
	char		buf[MAXDATELEN + 1];

	if (PTIMESTAMP_NOT_FINITE(dt))
		EncodeSpecialPTimestamp(dt, buf);
	else if (ptimestamp2tm(dt, &tz, tm, &fsec, &tzn, NULL) == 0)
		EncodeDateTime(tm, fsec, &tz, &tzn, DateStyle, buf);
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/*
 *		ptimestamptz_recv			- converts external binary format to timestamptz
 *
 * We make no attempt to provide compatibility between int and float
 * timestamp representations ...
 */

PG_FUNCTION_INFO_V1(ptimestamptz_recv);
Datum
ptimestamptz_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	PTimestampTz timestamp;
	PTimestampTz *result;
	int			tz;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	char	   *tzn;

#ifdef HAVE_INT64_TIMESTAMP
	timestamp = (PTimestampTz) pq_getmsgint64(buf);
#else
	timestamp = (PTimestampTz) pq_getmsgfloat8(buf);
#endif

	// rangecheck: see if ptimestamptz_out would like it 
	if (PTIMESTAMP_NOT_FINITE(timestamp)) ;
	else if (ptimestamp2tm(timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	
	result = (PTimestampTz *) palloc(sizeof(PTimestampTz));
	*result = timestamp;
	AdjustPTimestampForTypmod(result, typmod);

	PG_RETURN_PTIMESTAMPTZ(result);
}
/*
 *		ptimestamptz_send			- converts timestamptz to binary format
 */

PG_FUNCTION_INFO_V1(ptimestamptz_send);
Datum
ptimestamptz_send(PG_FUNCTION_ARGS)
{
	PTimestampTz	*timestamp  = (PTimestampTz *) PG_GETARG_PTIMESTAMPTZ(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
#ifdef HAVE_INT64_TIMESTAMP
	pq_sendint64(&buf, *timestamp);
#else
	pq_sendfloat8(&buf, *timestamp);
#endif
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
timestamptztypmodin(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);

	PG_RETURN_INT32(anytimestamp_typmodin(true, ta));
}

Datum
timestamptztypmodout(PG_FUNCTION_ARGS)
{
	int32		typmod = PG_GETARG_INT32(0);

	PG_RETURN_CSTRING(anytimestamp_typmodout(true, typmod));
}


/* ptimestamptz_scale()
 * Adjust time type for specified scale factor.
 * Used by PostgreSQL type system to stuff columns.
 */
PG_FUNCTION_INFO_V1(ptimestamptz_scale);
Datum
ptimestamptz_scale(PG_FUNCTION_ARGS)
{
	PTimestampTz	*timestamp= (PTimestampTz *) PG_GETARG_PTIMESTAMPTZ(0);
	int32		typmod = PG_GETARG_INT32(1);

	AdjustPTimestampForTypmod(timestamp, typmod);

	PG_RETURN_PTIMESTAMPTZ(timestamp);
}


/* EncodeSpecialPTimestamp()
 * Convert reserved timestamp data type to string.
 */
static int
EncodeSpecialPTimestamp(PTimestamp dt, char *str)
{
	if (PTIMESTAMP_IS_NOBEGIN(dt))
		strcpy(str, EARLY);
	else if (PTIMESTAMP_IS_NOEND(dt))
		strcpy(str, LATE);
	else
		return FALSE;

	return TRUE;
}	/* EncodeSpecialPTimestamp() */
/*
Datum
now(PG_FUNCTION_ARGS)
{
//TODO:
	PG_RETURN_PTIMESTAMPTZ(GetCurrentTransactionStartTimestamp());
}

Datum
statement_ptimestamp(PG_FUNCTION_ARGS)
{
//TODO:
	PG_RETURN_PTIMESTAMPTZ(GetCurrentStatementStartTimestamp());
}

Datum
clock_ptimestamp(PG_FUNCTION_ARGS)
{
	PG_RETURN_PTIMESTAMPTZ(GetCurrentPTimestamp());
}

Datum
pgsql_postmaster_start_time(PG_FUNCTION_ARGS)
{
	PG_RETURN_PTIMESTAMPTZ(PgStartTime_P);
}
*/
/*
 * GetCurrentPTimestamp -- get the current operating system time
 *
 * Result is in the form of a PTimestampTz value, and is expressed to the
 * full precision of the gettimeofday() syscall
 */
PTimestampTz
GetCurrentPTimestamp(void)
{
	PTimestampTz result;
	struct timeval tp;

	gettimeofday(&tp, NULL);

	result = (PTimestampTz) tp.tv_sec /*-
		((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)*/ * SECS_PER_DAY;

#ifdef HAVE_INT64_TIMESTAMP
	result = (result * USECS_PER_SEC) + tp.tv_usec;
#else
	result = result + (tp.tv_usec / 1000000.0);
#endif

	return result;
}

/*
 * PTimestampDifference -- convert the difference between two timestamps
 *		into integer seconds and microseconds
 *
 * Both inputs must be ordinary finite timestamps (in current usage,
 * they'll be results from GetCurrentPTimestamp()).
 *
 * We expect start_time <= stop_time.  If not, we return zeroes; for current
 * callers there is no need to be tense about which way division rounds on
 * negative inputs.
 */
void
PTimestampDifference(PTimestampTz start_time, PTimestampTz stop_time,
					long *secs, int *microsecs)
{
	PTimestampTz diff = stop_time - start_time;

	if (diff <= 0)
	{
		*secs = 0;
		*microsecs = 0;
	}
	else
	{
#ifdef HAVE_INT64_TIMESTAMP
		*secs = (long) (diff / USECS_PER_SEC);
		*microsecs = (int) (diff % USECS_PER_SEC);
#else
		*secs = (long) diff;
		*microsecs = (int) ((diff - *secs) * 1000000.0);
#endif
	}
}

/*
 * PTimestampDifferenceExceeds -- report whether the difference between two
 *		timestamps is >= a threshold (expressed in milliseconds)
 *
 * Both inputs must be ordinary finite timestamps (in current usage,
 * they'll be results from GetCurrentPTimestamp()).
 */
bool
PTimestampDifferenceExceeds(PTimestampTz start_time,
						   PTimestampTz stop_time,
						   int msec)
{
	PTimestampTz diff = stop_time - start_time;

#ifdef HAVE_INT64_TIMESTAMP
	return (diff >= msec * INT64CONST(1000));
#else
	return (diff * 1000.0 >= msec);
#endif
}

/*
 * Convert a time_t to PTimestampTz.
 *
 * We do not use time_t internally in Postgres, but this is provided for use
 * by functions that need to interpret, say, a stat(2) result.
 */
PTimestampTz
time_t_to_ptimestamptz(time_t tm)
{
	PTimestampTz result;

	result = (PTimestampTz) tm -
		((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

#ifdef HAVE_INT64_TIMESTAMP
	result *= USECS_PER_SEC;
#endif

	return result;
}

/*
 * Convert a PTimestampTz to time_t.
 *
 * This too is just marginally useful, but some places need it.
 */
time_t
ptimestamptz_to_time_t(PTimestampTz t)
{
	time_t		result;

#ifdef HAVE_INT64_TIMESTAMP
	result = (time_t) (t / USECS_PER_SEC +
				 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#else
	result = (time_t) (t +
				 ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#endif

	return result;
}

/*
 * Produce a C-string representation of a PTimestampTz.
 *
 * This is mostly for use in emitting messages.  The primary difference
 * from ptimestamptz_out is that we force the output format to ISO.	Note
 * also that the result is in a static buffer, not pstrdup'd.
 */
const char *
ptimestamptz_to_str(PTimestampTz t)
{
	static char buf[MAXDATELEN + 1];
	int			tz;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	char	   *tzn;

	if (TIMESTAMP_NOT_FINITE(t))
		EncodeSpecialPTimestamp(t, buf);
	else if (ptimestamp2tm(t, &tz, tm, &fsec, &tzn, NULL) == 0)
		EncodeDateTime(tm, fsec, &tz, &tzn, USE_ISO_DATES, buf);
	else
		strlcpy(buf, "(timestamp out of range)", sizeof(buf));

	return buf;
}


void
dt2time(PTimestamp jd, int *hour, int *min, int *sec, fsec_t *fsec)
{
#ifdef HAVE_INT64_TIMESTAMP
	int64		time;
#else
	double		time;
#endif

	time = jd;

#ifdef HAVE_INT64_TIMESTAMP
	*hour = time / USECS_PER_HOUR;
	time -= (*hour) * USECS_PER_HOUR;
	*min = time / USECS_PER_MINUTE;
	time -= (*min) * USECS_PER_MINUTE;
	*sec = time / USECS_PER_SEC;
	*fsec = time - (*sec * USECS_PER_SEC);
#else
	*hour = time / SECS_PER_HOUR;
	time -= (*hour) * SECS_PER_HOUR;
	*min = time / SECS_PER_MINUTE;
	time -= (*min) * SECS_PER_MINUTE;
	*sec = time;
	*fsec = time - *sec;
#endif
}	/* dt2time() */


/*
 * ptimestamp2tm() - Convert timestamp data type to POSIX time structure.
 *
 * Note that year is _not_ 1900-based, but is an explicit full value.
 * Also, month is one-based, _not_ zero-based.
 * Returns:
 *	 0 on success
 *	-1 on out of range
 *
 * If attimezone is NULL, the global timezone (including possibly brute forced
 * timezone) will be used.
 */
int
ptimestamp2tm(PTimestamp dt, int *tzp, struct pg_tm * tm, fsec_t *fsec, char **tzn, pg_tz *attimezone)
{
	PTimestamp	date;
	PTimestamp	time;
	pg_time_t	utime;

	/*
	 * If HasCTZSet is true then we have a brute force time zone specified. Go
	 * ahead and rotate to the local time zone since we will later bypass any
	 * calls which adjust the tm fields.
	 */
	if (attimezone == NULL && HasCTZSet && tzp != NULL)
	{
#ifdef HAVE_INT64_TIMESTAMP
		dt -= CTimeZone * USECS_PER_SEC;
#else
		dt -= CTimeZone;
#endif
	}

#ifdef HAVE_INT64_TIMESTAMP
	time = dt;
	TMODULO(time, date, USECS_PER_DAY);

	if (time < INT64CONST(0))
	{
		time += USECS_PER_DAY;
		date -= 1;
	}

	if (date < 0 || date > (PTimestamp) INT_MAX)
		return -1;

	p2date((int) date, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
	dt2time(time, &tm->tm_hour, &tm->tm_min, &tm->tm_sec, fsec);
#else
	time = dt;
	TMODULO(time, date, (double) SECS_PER_DAY);

	if (time < 0)
	{
		time += SECS_PER_DAY;
		date -= 1;
	}

recalc_d:
	//Maybe generates error:
	//TODO:if (date < 0 || date > (PTimestamp) INT_MAX)
	//	return -1;

	p2date((int) date, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
recalc_t:
	dt2time(time, &tm->tm_hour, &tm->tm_min, &tm->tm_sec, fsec);

	*fsec = TSROUND(*fsec);
	/* roundoff may need to propagate to higher-order fields */
	if (*fsec >= 1.0)
	{
		time = ceil(time);
		if (time >= (double) SECS_PER_DAY)
		{
			time = 0;
			date += 1;
			goto recalc_d;
		}
		goto recalc_t;
	}
#endif

	/* Done if no TZ conversion wanted */
	if (tzp == NULL)
	{
		tm->tm_isdst = -1;
		tm->tm_gmtoff = 0;
		tm->tm_zone = NULL;
		if (tzn != NULL)
			*tzn = NULL;
		return 0;
	}

	/*
	 * We have a brute force time zone per SQL99? Then use it without change
	 * since we have already rotated to the time zone.
	 */
	if (attimezone == NULL && HasCTZSet)
	{
		*tzp = CTimeZone;
		tm->tm_isdst = 0;
		tm->tm_gmtoff = CTimeZone;
		tm->tm_zone = NULL;
		if (tzn != NULL)
			*tzn = NULL;
		return 0;
	}

	/*
	 * If the time falls within the range of pg_time_t, use pg_localtime() to
	 * rotate to the local time zone.
	 *
	 * First, convert to an integral timestamp, avoiding possibly
	 * platform-specific roundoff-in-wrong-direction errors, and adjust to
	 * Unix epoch.	Then see if we can convert to pg_time_t without loss. This
	 * coding avoids hardwiring any assumptions about the width of pg_time_t,
	 * so it should behave sanely on machines without int64.
	 */
#ifdef HAVE_INT64_TIMESTAMP
	dt = (dt - *fsec) / USECS_PER_SEC /*+
		(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)*/ * SECS_PER_DAY;
#else
	dt = rint(dt - *fsec /*+
			  (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)*/ * SECS_PER_DAY);
#endif
/*
 * Fails in timestamptz
	utime = (pg_time_t) dt;
	if ((PTimestamp) utime == dt)
	{
		struct pg_tm *tx = pg_localtime(&utime,
								 attimezone ? attimezone : session_timezone);

		tm->tm_year = tx->tm_year + 1300; //TODO: 1900
		tm->tm_mon = tx->tm_mon + 1;
		tm->tm_mday = tx->tm_mday;
		tm->tm_hour = tx->tm_hour;
		tm->tm_min = tx->tm_min;
		tm->tm_sec = tx->tm_sec;
		tm->tm_isdst = tx->tm_isdst;
		tm->tm_gmtoff = tx->tm_gmtoff;
		tm->tm_zone = tx->tm_zone;
		*tzp = -tm->tm_gmtoff;
		if (tzn != NULL)
			*tzn = (char *) tm->tm_zone;
	}
	else
	{
		// When out of range of pg_time_t, treat as GMT
		*tzp = 0;
		tm->tm_isdst = -1;
		tm->tm_gmtoff = 0;
		tm->tm_zone = NULL;
		if (tzn != NULL)
			*tzn = NULL;
	}
*/
	return 0;
}


/* tm2ptimestamp()
 * Convert a tm structure to a timestamp data type.
 * Note that year is _not_ 1900-based, but is an explicit full value.
 * Also, month is one-based, _not_ zero-based.
 *
 * Returns -1 on failure (value out of range).
 */
int
tm2ptimestamp(struct pg_tm * tm, fsec_t fsec, int *tzp, PTimestamp *result)
{
#ifdef HAVE_INT64_TIMESTAMP
	int			date;
	int64		time;
#else
	double		date,
				time;
#endif

	/* Julian day routines are not correct for negative Julian days */
	if (!IS_VALID_PERSIAN(tm->tm_year, tm->tm_mon, tm->tm_mday))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}

	date = date2p(tm->tm_year, tm->tm_mon, tm->tm_mday); //TODO:- POSTGRES_EPOCH_JDATE;
	time = time2t(tm->tm_hour, tm->tm_min, tm->tm_sec, fsec);

#ifdef HAVE_INT64_TIMESTAMP
	*result = date * USECS_PER_DAY + time;
	/* check for major overflow */
	if ((*result - time) / USECS_PER_DAY != date)
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}
	/* check for just-barely overflow (okay except time-of-day wraps) */
	if ((*result < 0 && date >= 0) ||
		(*result >= 0 && date < 0))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}
#else
	*result = date * SECS_PER_DAY + time;
#endif
	if (tzp != NULL)
		*result = dt2local(*result, -(*tzp));

	return 0;
}

/*
int
tm2interval(struct pg_tm * tm, fsec_t fsec, Interval *span)
{
	span->month = tm->tm_year * MONTHS_PER_YEAR + tm->tm_mon;
	span->day = tm->tm_mday;
#ifdef HAVE_INT64_TIMESTAMP
	span->time = (((((tm->tm_hour * INT64CONST(60)) +
					 tm->tm_min) * INT64CONST(60)) +
				   tm->tm_sec) * USECS_PER_SEC) + fsec;
#else
	span->time = (((tm->tm_hour * (double) MINS_PER_HOUR) +
				   tm->tm_min) * (double) SECS_PER_MINUTE) +
		tm->tm_sec + fsec;
#endif

	return 0;
}
*/
#ifdef HAVE_INT64_TIMESTAMP
static int64
time2t(const int hour, const int min, const int sec, const fsec_t fsec)
{
	return (((((hour * MINS_PER_HOUR) + min) * SECS_PER_MINUTE) + sec) * USECS_PER_SEC) + fsec;
}	/* time2t() */
#else
static double
time2t(const int hour, const int min, const int sec, const fsec_t fsec)
{
	return (((hour * MINS_PER_HOUR) + min) * SECS_PER_MINUTE) + sec + fsec;
}	/* time2t() */
#endif

static PTimestamp
dt2local(PTimestamp dt, int tz)
{
#ifdef HAVE_INT64_TIMESTAMP
	dt -= (tz * USECS_PER_SEC);
#else
	dt -= tz;
#endif
	return dt;
}	/* dt2local() */


/*****************************************************************************
 *	 PUBLIC ROUTINES														 *
 *****************************************************************************/


Datum
ptimestamp_finite(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(0);

	PG_RETURN_BOOL(!PTIMESTAMP_NOT_FINITE(*timestamp));
}

Datum
interval_finite(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(true);
}


/*----------------------------------------------------------
 *	Relational operators for timestamp.
 *---------------------------------------------------------*/

void
GetEpochTime(struct pg_tm * tm)
{
	struct pg_tm *t0;
	pg_time_t	epoch = 0;

	t0 = pg_gmtime(&epoch);

	tm->tm_year = t0->tm_year;
	tm->tm_mon = t0->tm_mon;
	tm->tm_mday = t0->tm_mday;
	tm->tm_hour = t0->tm_hour;
	tm->tm_min = t0->tm_min;
	tm->tm_sec = t0->tm_sec;

	tm->tm_year += 1900;
	tm->tm_mon++;
}

PTimestamp
SetEpochPTimestamp(void)
{
	PTimestamp	dt;
	struct pg_tm tt,
			   *tm = &tt;

	GetEpochTime(tm);
	/* we don't bother to test for failure ... */
	tm2ptimestamp(tm, 0, NULL, &dt);

	return dt;
}	/* SetEpochPTimestamp() */

/*
 * We are currently sharing some code between timestamp and timestamptz.
 * The comparison functions are among them. - thomas 2001-09-25
 *
 *		ptimestamp_relop - is timestamp1 relop timestamp2
 *
 *		collate invalid timestamp at the end
 */
int
ptimestamp_cmp_internal(PTimestamp *dt3, PTimestamp *dt4)
{
	PTimestamp dt1 = *dt3;
	PTimestamp dt2 = *dt4;
#ifdef HAVE_INT64_TIMESTAMP
	return (dt1 < dt2) ? -1 : ((dt1 > dt2) ? 1 : 0);
#else

	/*
	 * When using float representation, we have to be wary of NaNs.
	 *
	 * We consider all NANs to be equal and larger than any non-NAN. This is
	 * somewhat arbitrary; the important thing is to have a consistent sort
	 * order.
	 */
	if (isnan(dt1))
	{
		if (isnan(dt2))
			return 0;			/* NAN = NAN */
		else
			return 1;			/* NAN > non-NAN */
	}
	else if (isnan(dt2))
	{
		return -1;				/* non-NAN < NAN */
	}
	else
	{
		if (dt1 > dt2)
			return 1;
		else if (dt1 < dt2)
			return -1;
		else
			return 0;
	}
#endif
}

/* 
 * Comparison functions for type ptimestamp
 */
PG_FUNCTION_INFO_V1(ptimestamp_eq);
Datum
ptimestamp_eq(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, dt2) == 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_ne);
Datum
ptimestamp_ne(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, dt2) != 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_lt);
Datum
ptimestamp_lt(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, dt2) < 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_gt);
Datum
ptimestamp_gt(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, dt2) > 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_le);
Datum
ptimestamp_le(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, dt2) <= 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_ge);
Datum
ptimestamp_ge(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, dt2) >= 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_cmp);
Datum
ptimestamp_cmp(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);

	PG_RETURN_INT32(ptimestamp_cmp_internal(dt1, dt2));
}

PG_FUNCTION_INFO_V1(ptimestamp_hash);
Datum
ptimestamp_hash(PG_FUNCTION_ARGS)
{
	/* We can use either hashint8 or hashfloat8 directly */
#ifdef HAVE_INT64_TIMESTAMP
	return hashint8(fcinfo);
#else
	return hashfloat8(fcinfo);
#endif
}


/*
 * Crosstype comparison functions for timestamp vs timestamptz
 */
PG_FUNCTION_INFO_V1(ptimestamp_eq_ptimestamptz);
Datum
ptimestamp_eq_ptimestamptz(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) == 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_ne_ptimestamptz);
Datum
ptimestamp_ne_ptimestamptz(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) != 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_lt_ptimestamptz);
Datum
ptimestamp_lt_ptimestamptz(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) < 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_gt_ptimestamptz);
Datum
ptimestamp_gt_ptimestamptz(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) > 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_le_ptimestamptz);
Datum
ptimestamp_le_ptimestamptz(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) <= 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_ge_ptimestamptz);
Datum
ptimestamp_ge_ptimestamptz(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(&dt1, dt2) >= 0);
}

PG_FUNCTION_INFO_V1(ptimestamp_cmp_ptimestamptz);
Datum
ptimestamp_cmp_ptimestamptz(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz dt1;

	dt1 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_INT32(ptimestamp_cmp_internal(&dt1, dt2));
}

PG_FUNCTION_INFO_V1(ptimestamptz_eq_ptimestamp);
Datum
ptimestamptz_eq_ptimestamp(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(1);
	PTimestampTz dt2;

	dt2 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) == 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_ne_ptimestamp);
Datum
ptimestamptz_ne_ptimestamp(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(1);
	PTimestampTz dt2;

	dt2 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) != 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_lt_ptimestamp);
Datum
ptimestamptz_lt_ptimestamp(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(1);
	PTimestampTz dt2;

	dt2 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) < 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_gt_ptimestamp);
Datum
ptimestamptz_gt_ptimestamp(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(1);
	PTimestampTz dt2;

	dt2 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) > 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_le_ptimestamp);
Datum
ptimestamptz_le_ptimestamp(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(1);
	PTimestampTz dt2;

	dt2 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) <= 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_ge_ptimestamp);
Datum
ptimestamptz_ge_ptimestamp(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(1);
	PTimestampTz dt2;

	dt2 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_BOOL(ptimestamp_cmp_internal(dt1, &dt2) >= 0);
}

PG_FUNCTION_INFO_V1(ptimestamptz_cmp_ptimestamp);
Datum
ptimestamptz_cmp_ptimestamp(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PTimestamp	*timestampVal = PG_GETARG_PTIMESTAMP(1);
	PTimestampTz dt2;

	dt2 = ptimestamp2ptimestamptz(*timestampVal);

	PG_RETURN_INT32(ptimestamp_cmp_internal(dt1, &dt2));
}

/* overlaps_ptimestamp() --- implements the SQL92 OVERLAPS operator.
 *
 * Algorithm is per SQL92 spec.  This is much harder than you'd think
 * because the spec requires us to deliver a non-null answer in some cases
 * where some of the inputs are null.
 */
PG_FUNCTION_INFO_V1(overlaps_ptimestamp);
Datum
overlaps_ptimestamp(PG_FUNCTION_ARGS)
{
	/*
	 * The arguments are PTimestamps, but we leave them as generic Datums to
	 * avoid unnecessary conversions between value and reference forms --- not
	 * to mention possible dereferences of null pointers.
	 */
	Datum		ts1 = PG_GETARG_DATUM(0);
	Datum		te1 = PG_GETARG_DATUM(1);
	Datum		ts2 = PG_GETARG_DATUM(2);
	Datum		te2 = PG_GETARG_DATUM(3);
	bool		ts1IsNull = PG_ARGISNULL(0);
	bool		te1IsNull = PG_ARGISNULL(1);
	bool		ts2IsNull = PG_ARGISNULL(2);
	bool		te2IsNull = PG_ARGISNULL(3);

#define PTIMESTAMP_GT(t1,t2) \
	DatumGetBool(DirectFunctionCall2(ptimestamp_gt,t1,t2))
#define PTIMESTAMP_LT(t1,t2) \
	DatumGetBool(DirectFunctionCall2(ptimestamp_lt,t1,t2))

	/*
	 * If both endpoints of interval 1 are null, the result is null (unknown).
	 * If just one endpoint is null, take ts1 as the non-null one. Otherwise,
	 * take ts1 as the lesser endpoint.
	 */
	if (ts1IsNull)
	{
		if (te1IsNull)
			PG_RETURN_NULL();
		/* swap null for non-null */
		ts1 = te1;
		te1IsNull = true;
	}
	else if (!te1IsNull)
	{
		if (PTIMESTAMP_GT(ts1, te1))
		{
			Datum		tt = ts1;

			ts1 = te1;
			te1 = tt;
		}
	}

	/* Likewise for interval 2. */
	if (ts2IsNull)
	{
		if (te2IsNull)
			PG_RETURN_NULL();
		/* swap null for non-null */
		ts2 = te2;
		te2IsNull = true;
	}
	else if (!te2IsNull)
	{
		if (PTIMESTAMP_GT(ts2, te2))
		{
			Datum		tt = ts2;

			ts2 = te2;
			te2 = tt;
		}
	}

	/*
	 * At this point neither ts1 nor ts2 is null, so we can consider three
	 * cases: ts1 > ts2, ts1 < ts2, ts1 = ts2
	 */
	if (PTIMESTAMP_GT(ts1, ts2))
	{
		/*
		 * This case is ts1 < te2 OR te1 < te2, which may look redundant but
		 * in the presence of nulls it's not quite completely so.
		 */
		if (te2IsNull)
			PG_RETURN_NULL();
		if (PTIMESTAMP_LT(ts1, te2))
			PG_RETURN_BOOL(true);
		if (te1IsNull)
			PG_RETURN_NULL();

		/*
		 * If te1 is not null then we had ts1 <= te1 above, and we just found
		 * ts1 >= te2, hence te1 >= te2.
		 */
		PG_RETURN_BOOL(false);
	}
	else if (PTIMESTAMP_LT(ts1, ts2))
	{
		/* This case is ts2 < te1 OR te2 < te1 */
		if (te1IsNull)
			PG_RETURN_NULL();
		if (PTIMESTAMP_LT(ts2, te1))
			PG_RETURN_BOOL(true);
		if (te2IsNull)
			PG_RETURN_NULL();

		/*
		 * If te2 is not null then we had ts2 <= te2 above, and we just found
		 * ts2 >= te1, hence te2 >= te1.
		 */
		PG_RETURN_BOOL(false);
	}
	else
	{
		/*
		 * For ts1 = ts2 the spec says te1 <> te2 OR te1 = te2, which is a
		 * rather silly way of saying "true if both are nonnull, else null".
		 */
		if (te1IsNull || te2IsNull)
			PG_RETURN_NULL();
		PG_RETURN_BOOL(true);
	}

#undef PTIMESTAMP_GT
#undef PTIMESTAMP_LT
}


/*----------------------------------------------------------
 *	"Arithmetic" operators on date/times.
 *---------------------------------------------------------*/
PG_FUNCTION_INFO_V1(ptimestamp_smaller);
Datum
ptimestamp_smaller(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);
	PTimestamp	*result;

	result = (PTimestamp *) palloc(sizeof(PTimestamp));
	// use ptimestamp_cmp_internal to be sure this agrees with comparisons 
	if (ptimestamp_cmp_internal(dt1, dt2) < 0)
		*result = *dt1;
	else
		*result = *dt2;
	PG_RETURN_PTIMESTAMP(result);
}

PG_FUNCTION_INFO_V1(ptimestamp_larger);
Datum
ptimestamp_larger(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);
	PTimestamp	*result;

	result = (PTimestamp *) palloc(sizeof(PTimestamp));
	if (ptimestamp_cmp_internal(dt1, dt2) > 0)
		*result = *dt1;
	else
		*result = *dt2;
	PG_RETURN_PTIMESTAMP(result);
}


PG_FUNCTION_INFO_V1(ptimestamp_mi);
Datum
ptimestamp_mi(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	if (PTIMESTAMP_NOT_FINITE(*dt1) || PTIMESTAMP_NOT_FINITE(*dt2))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("cannot subtract infinite timestamps")));

	result->time = *dt1 - *dt2;

	result->month = 0;
	result->day = 0;
	/*----------
	 *	This is wrong, but removing it breaks a lot of regression tests.
	 *	For example:
	 *
	 *	test=> SET timezone = 'EST5EDT';
	 *	test=> SELECT
	 *	test-> ('2005-10-30 13:22:00-05'::timestamptz -
	 *	test(>	'2005-10-29 13:22:00-04'::timestamptz);
	 *	?column?
	 *	----------------
	 *	 1 day 01:00:00
	 *	 (1 row)
	 *
	 *	so adding that to the first timestamp gets:
	 *
	 *	 test=> SELECT
	 *	 test-> ('2005-10-29 13:22:00-04'::timestamptz +
	 *	 test(> ('2005-10-30 13:22:00-05'::timestamptz -
	 *	 test(>  '2005-10-29 13:22:00-04'::timestamptz)) at time zone 'EST';
	 *		timezone
	 *	--------------------
	 *	2005-10-30 14:22:00
	 *	(1 row)
	 *----------
	 */
	result = DatumGetIntervalP(DirectFunctionCall1(interval_justify_hours,
												 IntervalPGetDatum(result)));

	PG_RETURN_INTERVAL_P(result);
}

/* ptimestamp_pl_interval()
 * Add a interval to a timestamp data type.
 * Note that interval has provisions for qualitative year/month and day
 *	units, so try to do the right thing with them.
 * To add a month, increment the month, and use the same day of month.
 * Then, if the next month has fewer days, set the day of month
 *	to the last day of month.
 * To add a day, increment the mday, and use the same time of day.
 * Lastly, add in the "quantitative time".
 */
PG_FUNCTION_INFO_V1(ptimestamp_pl_interval);
Datum
ptimestamp_pl_interval(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	PTimestamp	*result;

	result = (PTimestampTz *) palloc(sizeof(PTimestampTz));
	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		result = timestamp;
	else
	{
		if (span->month != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;

			if (ptimestamp2tm(*timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			tm->tm_mon += span->month;
			if (tm->tm_mon > MONTHS_PER_YEAR)
			{
				tm->tm_year += (tm->tm_mon - 1) / MONTHS_PER_YEAR;
				tm->tm_mon = ((tm->tm_mon - 1) % MONTHS_PER_YEAR) + 1;
			}
			else if (tm->tm_mon < 1)
			{
				tm->tm_year += tm->tm_mon / MONTHS_PER_YEAR - 1;
				tm->tm_mon = tm->tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
			}

			// adjust for end of month boundary problems... //
			if (tm->tm_mday > pday_tab[p_isleap(tm->tm_year)][tm->tm_mon - 1])
				tm->tm_mday = (pday_tab[p_isleap(tm->tm_year)][tm->tm_mon - 1]);

			if (tm2ptimestamp(tm, fsec, NULL, timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		if (span->day != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;
			int			julian;

			if (ptimestamp2tm(*timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			// Add days by converting to and from julian 
			julian = date2p(tm->tm_year, tm->tm_mon, tm->tm_mday) + span->day;
			p2date(julian, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);

			if (tm2ptimestamp(tm, fsec, NULL, timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		*timestamp += span->time;
		result = timestamp;
	}

	PG_RETURN_PTIMESTAMP(result);
}

PG_FUNCTION_INFO_V1(ptimestamp_mi_interval);
Datum
ptimestamp_mi_interval(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	Interval	tspan;

	tspan.month = -span->month;
	tspan.day = -span->day;
	tspan.time = -span->time;

	return DirectFunctionCall2(ptimestamp_pl_interval,
							   PTimestampGetDatum(*timestamp),
							   PointerGetDatum(&tspan));
}

/* ptimestamptz_pl_interval()
 * Add a interval to a timestamp with time zone data type.
 * Note that interval has provisions for qualitative year/month
 *	units, so try to do the right thing with them.
 * To add a month, increment the month, and use the same day of month.
 * Then, if the next month has fewer days, set the day of month
 *	to the last day of month.
 * Lastly, add in the "quantitative time".
 */
PG_FUNCTION_INFO_V1(ptimestamptz_pl_interval);
Datum
ptimestamptz_pl_interval(PG_FUNCTION_ARGS)
{
	PTimestampTz *timestamp = PG_GETARG_PTIMESTAMPTZ(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	PTimestampTz *result;
	int			tz;
	char	   *tzn;

	result = (PTimestampTz *) palloc(sizeof(PTimestampTz));
	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		result = timestamp;
	else
	{
		if (span->month != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;

			if (ptimestamp2tm(*timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			tm->tm_mon += span->month;
			if (tm->tm_mon > MONTHS_PER_YEAR)
			{
				tm->tm_year += (tm->tm_mon - 1) / MONTHS_PER_YEAR;
				tm->tm_mon = ((tm->tm_mon - 1) % MONTHS_PER_YEAR) + 1;
			}
			else if (tm->tm_mon < 1)
			{
				tm->tm_year += tm->tm_mon / MONTHS_PER_YEAR - 1;
				tm->tm_mon = tm->tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
			}

			// adjust for end of month boundary problems... 
			if (tm->tm_mday > pday_tab[p_isleap(tm->tm_year)][tm->tm_mon - 1])
				tm->tm_mday = (pday_tab[p_isleap(tm->tm_year)][tm->tm_mon - 1]);

			tz = P_DetermineTimeZoneOffset(tm, session_timezone);

			if (tm2ptimestamp(tm, fsec, &tz, timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		if (span->day != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;
			int			julian;

			if (ptimestamp2tm(*timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			// Add days by converting to and from julian 
			julian = date2p(tm->tm_year, tm->tm_mon, tm->tm_mday) + span->day;
			p2date(julian, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);

			tz = P_DetermineTimeZoneOffset(tm, session_timezone);

			if (tm2ptimestamp(tm, fsec, &tz, timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		*timestamp += span->time;
		result = timestamp;
	}

	PG_RETURN_PTIMESTAMP(result);
}

PG_FUNCTION_INFO_V1(ptimestamptz_mi_interval);
Datum
ptimestamptz_mi_interval(PG_FUNCTION_ARGS)
{
	PTimestampTz *timestamp = PG_GETARG_PTIMESTAMPTZ(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	Interval	tspan;

	tspan.month = -span->month;
	tspan.day = -span->day;
	tspan.time = -span->time;

	return DirectFunctionCall2(ptimestamptz_pl_interval,
							   PTimestampGetDatum(*timestamp),
							   PointerGetDatum(&tspan));
}

/* ptimestamp_age()
 * Calculate time difference while retaining year/month fields.
 * Note that this does not result in an accurate absolute time span
 *	since year and month are out of context once the arithmetic
 *	is done.
 */
PG_FUNCTION_INFO_V1(ptimestamp_age);
Datum
ptimestamp_age(PG_FUNCTION_ARGS)
{
	PTimestamp	*dt1 = PG_GETARG_PTIMESTAMP(0);
	PTimestamp	*dt2 = PG_GETARG_PTIMESTAMP(1);
	Interval   *result;
	fsec_t		fsec,
				fsec1,
				fsec2;
	struct pg_tm tt,
			   *tm = &tt;
	struct pg_tm tt1,
			   *tm1 = &tt1;
	struct pg_tm tt2,
			   *tm2 = &tt2;

	result = (Interval *) palloc(sizeof(Interval));

	if (ptimestamp2tm(*dt1, NULL, tm1, &fsec1, NULL, NULL) == 0 &&
		ptimestamp2tm(*dt2, NULL, tm2, &fsec2, NULL, NULL) == 0)
	{
		// form the symbolic difference 
		fsec = fsec1 - fsec2;
		tm->tm_sec = tm1->tm_sec - tm2->tm_sec;
		tm->tm_min = tm1->tm_min - tm2->tm_min;
		tm->tm_hour = tm1->tm_hour - tm2->tm_hour;
		tm->tm_mday = tm1->tm_mday - tm2->tm_mday;
		tm->tm_mon = tm1->tm_mon - tm2->tm_mon;
		tm->tm_year = tm1->tm_year - tm2->tm_year;

		// flip sign if necessary... 
		if (*dt1 < *dt2)
		{
			fsec = -fsec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		// propagate any negative fields into the next higher field 
		while (fsec < 0)
		{
#ifdef HAVE_INT64_TIMESTAMP
			fsec += USECS_PER_SEC;
#else
			fsec += 1.0;
#endif
			tm->tm_sec--;
		}

		while (tm->tm_sec < 0)
		{
			tm->tm_sec += SECS_PER_MINUTE;
			tm->tm_min--;
		}

		while (tm->tm_min < 0)
		{
			tm->tm_min += MINS_PER_HOUR;
			tm->tm_hour--;
		}

		while (tm->tm_hour < 0)
		{
			tm->tm_hour += HOURS_PER_DAY;
			tm->tm_mday--;
		}

		while (tm->tm_mday < 0)
		{
			if (dt1 < dt2)
			{
				tm->tm_mday += pday_tab[p_isleap(tm1->tm_year)][tm1->tm_mon - 1];
				tm->tm_mon--;
			}
			else
			{
				tm->tm_mday += pday_tab[p_isleap(tm2->tm_year)][tm2->tm_mon - 1];
				tm->tm_mon--;
			}
		}

		while (tm->tm_mon < 0)
		{
			tm->tm_mon += MONTHS_PER_YEAR;
			tm->tm_year--;
		}

		// recover sign if necessary... 
		if (*dt1 < *dt2)
		{
			fsec = -fsec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		if (tm2interval(tm, fsec, result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_INTERVAL_P(result);
}

/* ptimestamptz_age()
 * Calculate time difference while retaining year/month fields.
 * Note that this does not result in an accurate absolute time span
 *	since year and month are out of context once the arithmetic
 *	is done.
 */
PG_FUNCTION_INFO_V1(ptimestamptz_age);
Datum
ptimestamptz_age(PG_FUNCTION_ARGS)
{
	PTimestampTz *dt1 = PG_GETARG_PTIMESTAMPTZ(0);
	PTimestampTz *dt2 = PG_GETARG_PTIMESTAMPTZ(1);
	Interval   *result;
	fsec_t		fsec,
				fsec1,
				fsec2;
	struct pg_tm tt,
			   *tm = &tt;
	struct pg_tm tt1,
			   *tm1 = &tt1;
	struct pg_tm tt2,
			   *tm2 = &tt2;
	int			tz1;
	int			tz2;
	char	   *tzn;

	result = (Interval *) palloc(sizeof(Interval));

	if (ptimestamp2tm(*dt1, &tz1, tm1, &fsec1, &tzn, NULL) == 0 &&
		ptimestamp2tm(*dt2, &tz2, tm2, &fsec2, &tzn, NULL) == 0)
	{
		// form the symbolic difference 
		fsec = fsec1 - fsec2;
		tm->tm_sec = tm1->tm_sec - tm2->tm_sec;
		tm->tm_min = tm1->tm_min - tm2->tm_min;
		tm->tm_hour = tm1->tm_hour - tm2->tm_hour;
		tm->tm_mday = tm1->tm_mday - tm2->tm_mday;
		tm->tm_mon = tm1->tm_mon - tm2->tm_mon;
		tm->tm_year = tm1->tm_year - tm2->tm_year;

		// flip sign if necessary... 
		if (*dt1 < *dt2)
		{
			fsec = -fsec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		// propagate any negative fields into the next higher field 
		while (fsec < 0)
		{
#ifdef HAVE_INT64_TIMESTAMP
			fsec += USECS_PER_SEC;
#else
			fsec += 1.0;
#endif
			tm->tm_sec--;
		}

		while (tm->tm_sec < 0)
		{
			tm->tm_sec += SECS_PER_MINUTE;
			tm->tm_min--;
		}

		while (tm->tm_min < 0)
		{
			tm->tm_min += MINS_PER_HOUR;
			tm->tm_hour--;
		}

		while (tm->tm_hour < 0)
		{
			tm->tm_hour += HOURS_PER_DAY;
			tm->tm_mday--;
		}

		while (tm->tm_mday < 0)
		{
			if (dt1 < dt2)
			{
				tm->tm_mday += pday_tab[p_isleap(tm1->tm_year)][tm1->tm_mon - 1];
				tm->tm_mon--;
			}
			else
			{
				tm->tm_mday += pday_tab[p_isleap(tm2->tm_year)][tm2->tm_mon - 1];
				tm->tm_mon--;
			}
		}

		while (tm->tm_mon < 0)
		{
			tm->tm_mon += MONTHS_PER_YEAR;
			tm->tm_year--;
		}

		
		 // Note: we deliberately ignore any difference between tz1 and tz2.
		 

		// recover sign if necessary... 
		if (dt1 < dt2)
		{
			fsec = -fsec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		if (tm2interval(tm, fsec, result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_INTERVAL_P(result);
}

/*----------------------------------------------------------
 *	Conversion operators.
 *---------------------------------------------------------*/

/* ptimestamp_trunc()
 * Truncate timestamp to specified units.
 */
PG_FUNCTION_INFO_V1(ptimestamp_trunc);
Datum
ptimestamp_trunc(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_P(0);
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(1);
	PTimestamp	*result;
	int			type,
				val;
	char	   *lowunits;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	result = (PTimestamp *) palloc(sizeof(PTimestamp));
	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_PTIMESTAMP(timestamp);

	lowunits = downcase_truncate_identifier(VARDATA(units),
											VARSIZE(units) - VARHDRSZ,
											false);

	type = DecodeUnits(0, lowunits, &val);

	if (type == UNITS)
	{
		if (ptimestamp2tm(*timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_WEEK:
				{
					int			woy;

					woy = date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);

					if (woy >= 52 && tm->tm_mon == 1)
						--tm->tm_year;
					if (woy <= 1 && tm->tm_mon == MONTHS_PER_YEAR)
						++tm->tm_year;
					isoweek2date(woy, &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
					tm->tm_hour = 0;
					tm->tm_min = 0;
					tm->tm_sec = 0;
					fsec = 0;
					break;
				}
			case DTK_MILLENNIUM:
				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 999) / 1000) * 1000 - 999;
				else
					tm->tm_year = -((999 - (tm->tm_year - 1)) / 1000) * 1000 + 1;
			case DTK_CENTURY:
				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 99) / 100) * 100 - 99;
				else
					tm->tm_year = -((99 - (tm->tm_year - 1)) / 100) * 100 + 1;
			case DTK_DECADE:
				if (val != DTK_MILLENNIUM && val != DTK_CENTURY)
				{
					if (tm->tm_year > 0)
						tm->tm_year = (tm->tm_year / 10) * 10;
					else
						tm->tm_year = -((8 - (tm->tm_year - 1)) / 10) * 10;
				}
			case DTK_YEAR:
				tm->tm_mon = 1;
			case DTK_QUARTER:
				tm->tm_mon = (3 * ((tm->tm_mon - 1) / 3)) + 1;
			case DTK_MONTH:
				tm->tm_mday = 1;
			case DTK_DAY:
				tm->tm_hour = 0;
			case DTK_HOUR:
				tm->tm_min = 0;
			case DTK_MINUTE:
				tm->tm_sec = 0;
			case DTK_SECOND:
				fsec = 0;
				break;

			case DTK_MILLISEC:
#ifdef HAVE_INT64_TIMESTAMP
				fsec = (fsec / 1000) * 1000;
#else
				fsec = rint(fsec * 1000) / 1000;
#endif
				break;

			case DTK_MICROSEC:
#ifndef HAVE_INT64_TIMESTAMP
				fsec = rint(fsec * 1000000) / 1000000;
#endif
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp units \"%s\" not supported",
								lowunits)));
				*result = 0;
		}

		if (tm2ptimestamp(tm, fsec, NULL, result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("timestamp units \"%s\" not recognized",
						lowunits)));
		*result = 0;
	}

	PG_RETURN_PTIMESTAMP(result);
}
/* ptimestamptz_trunc()
 * Truncate timestamp to specified units.
 */
PG_FUNCTION_INFO_V1(ptimestamptz_trunc);
Datum
ptimestamptz_trunc(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_P(0);
	PTimestampTz *timestamp = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestampTz *result;
	int			tz;
	int			type,
				val;
	bool		redotz = false;
	char	   *lowunits;
	fsec_t		fsec;
	char	   *tzn;
	struct pg_tm tt,
			   *tm = &tt;

	result = (PTimestampTz *) palloc(sizeof(PTimestampTz));
	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_PTIMESTAMPTZ(timestamp);

	lowunits = downcase_truncate_identifier(VARDATA(units),
											VARSIZE(units) - VARHDRSZ,
											false);

	type = DecodeUnits(0, lowunits, &val);

	if (type == UNITS)
	{
		if (ptimestamp2tm(*timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_WEEK:
				{
					int			woy;

					woy = date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);

					if (woy >= 52 && tm->tm_mon == 1)
						--tm->tm_year;
					if (woy <= 1 && tm->tm_mon == MONTHS_PER_YEAR)
						++tm->tm_year;
					isoweek2date(woy, &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
					tm->tm_hour = 0;
					tm->tm_min = 0;
					tm->tm_sec = 0;
					fsec = 0;
					redotz = true;
					break;
				}
			case DTK_MILLENNIUM:

				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 999) / 1000) * 1000 - 999;
				else
					tm->tm_year = -((999 - (tm->tm_year - 1)) / 1000) * 1000 + 1;
			case DTK_CENTURY:
				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 99) / 100) * 100 - 99;
				else
					tm->tm_year = -((99 - (tm->tm_year - 1)) / 100) * 100 + 1;
			case DTK_DECADE:

				if (val != DTK_MILLENNIUM && val != DTK_CENTURY)
				{
					if (tm->tm_year > 0)
						tm->tm_year = (tm->tm_year / 10) * 10;
					else
						tm->tm_year = -((8 - (tm->tm_year - 1)) / 10) * 10;
				}
			case DTK_YEAR:
				tm->tm_mon = 1;
			case DTK_QUARTER:
				tm->tm_mon = (3 * ((tm->tm_mon - 1) / 3)) + 1;
			case DTK_MONTH:
				tm->tm_mday = 1;
			case DTK_DAY:
				tm->tm_hour = 0;
				redotz = true;	
			case DTK_HOUR:
				tm->tm_min = 0;
			case DTK_MINUTE:
				tm->tm_sec = 0;
			case DTK_SECOND:
				fsec = 0;
				break;

			case DTK_MILLISEC:
#ifdef HAVE_INT64_TIMESTAMP
				fsec = (fsec / 1000) * 1000;
#else
				fsec = rint(fsec * 1000) / 1000;
#endif
				break;
			case DTK_MICROSEC:
#ifndef HAVE_INT64_TIMESTAMP
				fsec = rint(fsec * 1000000) / 1000000;
#endif
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp with time zone units \"%s\" not "
								"supported", lowunits)));
				*result = 0;
		}

		if (redotz)
			tz = P_DetermineTimeZoneOffset(tm, session_timezone);

		if (tm2ptimestamp(tm, fsec, &tz, result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			   errmsg("timestamp with time zone units \"%s\" not recognized",
					  lowunits)));
		*result = 0;
	}

	PG_RETURN_PTIMESTAMPTZ(result);
}

/* isoweek2j()
 *
 *	Return the Julian day which corresponds to the first day (Monday) of the given ISO 8601 year and week.
 *	Julian days are used to convert between ISO week dates and Gregorian dates.
 */
int
isoweek2j(int year, int week)
{
	int			day0,
				day4;

	if (!year)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("cannot calculate week number without year information")));

	/* fourth day of current year */
	day4 = date2p(year, 1, 4);

	/* day0 == offset to first day of week (Monday) */
	day0 = p2day(day4 - 1);

	return ((week - 1) * 7) + (day4 - day0);
}

/* isoweek2date()
 * Convert ISO week of year number to date.
 * The year field must be specified with the ISO year!
 * karel 2000/08/07
 */
void
isoweek2date(int woy, int *year, int *mon, int *mday)
{
	p2date(isoweek2j(*year, woy), year, mon, mday);
}

/* isoweekdate2date()
 *
 *	Convert an ISO 8601 week date (ISO year, ISO week and day of week) into a Gregorian date.
 *	Populates year, mon, and mday with the correct Gregorian values.
 *	year must be passed in as the ISO year.
 */
void
isoweekdate2date(int isoweek, int isowday, int *year, int *mon, int *mday)
{
	int			jday;

	jday = isoweek2j(*year, isoweek);
	jday += isowday - 1;

	p2date(jday, year, mon, mday);
}

/* date2isoweek()
 *
 *	Returns ISO week number of year.
 */
int
date2isoweek(int year, int mon, int mday)
{
	float8		result;
	int			day0,
				day4,
				dayn;

	/* current day */
	dayn = date2p(year, mon, mday);

	/* fourth day of current year */
	day4 = date2p(year, 1, 4);

	/* day0 == offset to first day of week (Monday) */
	day0 = p2day(day4 - 1);

	/*
	 * We need the first week containing a Thursday, otherwise this day falls
	 * into the previous year for purposes of counting weeks
	 */
	if (dayn < day4 - day0)
	{
		day4 = date2p(year - 1, 1, 4);

		/* day0 == offset to first day of week (Monday) */
		day0 = p2day(day4 - 1);
	}

	result = (dayn - (day4 - day0)) / 7 + 1;

	/*
	 * Sometimes the last few days in a year will fall into the first week of
	 * the next year, so check for this.
	 */
	if (result >= 52)
	{
		day4 = date2p(year + 1, 1, 4);

		/* day0 == offset to first day of week (Monday) */
		day0 = p2day(day4 - 1);

		if (dayn >= day4 - day0)
			result = (dayn - (day4 - day0)) / 7 + 1;
	}

	return (int) result;
}


/* date2isoyear()
 *
 *	Returns ISO 8601 year number.
 */
int
date2isoyear(int year, int mon, int mday)
{
	float8		result;
	int			day0,
				day4,
				dayn;

	/* current day */
	dayn = date2p(year, mon, mday);

	/* fourth day of current year */
	day4 = date2p(year, 1, 4);

	/* day0 == offset to first day of week (Monday) */
	day0 = p2day(day4 - 1);

	/*
	 * We need the first week containing a Thursday, otherwise this day falls
	 * into the previous year for purposes of counting weeks
	 */
	if (dayn < day4 - day0)
	{
		day4 = date2p(year - 1, 1, 4);

		/* day0 == offset to first day of week (Monday) */
		day0 = p2day(day4 - 1);

		year--;
	}

	result = (dayn - (day4 - day0)) / 7 + 1;

	/*
	 * Sometimes the last few days in a year will fall into the first week of
	 * the next year, so check for this.
	 */
	if (result >= 52)
	{
		day4 = date2p(year + 1, 1, 4);

		/* day0 == offset to first day of week (Monday) */
		day0 = p2day(day4 - 1);

		if (dayn >= day4 - day0)
			year++;
	}

	return year;
}


/* date2isoyearday()
 *
 *	Returns the ISO 8601 day-of-year, given a Gregorian year, month and day.
 *	Possible return values are 1 through 371 (364 in non-leap years).
 */
int
date2isoyearday(int year, int mon, int mday)
{
	return date2p(year, mon, mday) - isoweek2j(date2isoyear(year, mon, mday), 1) + 1;
}

/* ptimestamp_part()
 * Extract specified field from timestamp.
 */
PG_FUNCTION_INFO_V1(ptimestamp_part);
Datum
ptimestamp_part(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_P(0);
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(1);
	float8		result;
	int			type,
				val;
	char	   *lowunits;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	if (PTIMESTAMP_NOT_FINITE(*timestamp))
	{
		result = 0;
		PG_RETURN_FLOAT8(result);
	}

	lowunits = downcase_truncate_identifier(VARDATA(units),
											VARSIZE(units) - VARHDRSZ,
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (type == UNITS)
	{
		if (ptimestamp2tm(*timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_MICROSEC:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec * 1000000.0 + fsec;
#else
				result = (tm->tm_sec + fsec) * 1000000;
#endif
				break;

			case DTK_MILLISEC:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec * 1000.0 + fsec / 1000.0;
#else
				result = (tm->tm_sec + fsec) * 1000;
#endif
				break;

			case DTK_SECOND:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec + fsec / 1000000.0;
#else
				result = tm->tm_sec + fsec;
#endif
				break;

			case DTK_MINUTE:
				result = tm->tm_min;
				break;

			case DTK_HOUR:
				result = tm->tm_hour;
				break;

			case DTK_DAY:
				result = tm->tm_mday;
				break;

			case DTK_MONTH:
				result = tm->tm_mon;
				break;

			case DTK_QUARTER:
				result = (tm->tm_mon - 1) / 3 + 1;
				break;

			case DTK_WEEK:
				result = (float8) date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);
				break;

			case DTK_YEAR:
				if (tm->tm_year > 0)
					result = tm->tm_year;
				else
					// there is no year 0, just 1 BC and 1 AD 
					result = tm->tm_year - 1;
				break;

			case DTK_DECADE:

				//
				 // what is a decade wrt dates? let us assume that decade 199
				 // is 1990 thru 1999... decade 0 starts on year 1 BC, and -1
				// is 11 BC thru 2 BC...
				 
				if (tm->tm_year >= 0)
					result = tm->tm_year / 10;
				else
					result = -((8 - (tm->tm_year - 1)) / 10);
				break;

			case DTK_CENTURY:

				// ----
				 // centuries AD, c>0: year in [ (c-1)* 100 + 1 : c*100 ]
				 // centuries BC, c<0: year in [ c*100 : (c+1) * 100 - 1]
				 // there is no number 0 century.
				 // ----
				 //
				if (tm->tm_year > 0)
					result = (tm->tm_year + 99) / 100;
				else
					// caution: C division may have negative remainder 
					result = -((99 - (tm->tm_year - 1)) / 100);
				break;

			case DTK_MILLENNIUM:
				// see comments above. 
				if (tm->tm_year > 0)
					result = (tm->tm_year + 999) / 1000;
				else
					result = -((999 - (tm->tm_year - 1)) / 1000);
				break;

			case DTK_JULIAN:
				result = date2p(tm->tm_year, tm->tm_mon, tm->tm_mday);
#ifdef HAVE_INT64_TIMESTAMP
				result += ((((tm->tm_hour * MINS_PER_HOUR) + tm->tm_min) * SECS_PER_MINUTE) +
					tm->tm_sec + (fsec / 1000000.0)) / (double) SECS_PER_DAY;
#else
				result += ((((tm->tm_hour * MINS_PER_HOUR) + tm->tm_min) * SECS_PER_MINUTE) +
						   tm->tm_sec + fsec) / (double) SECS_PER_DAY;
#endif
				break;

			case DTK_ISOYEAR:
				result = date2isoyear(tm->tm_year, tm->tm_mon, tm->tm_mday);
				break;

			case DTK_TZ:
			case DTK_TZ_MINUTE:
			case DTK_TZ_HOUR:
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp units \"%s\" not supported",
								lowunits)));
				result = 0;
		}
	}
	else if (type == RESERV)
	{
		switch (val)
		{
			case DTK_EPOCH:
				{
					int			tz;
					PTimestampTz timestamptz;

					//
					 // convert to timestamptz to produce consistent results
					 
					if (ptimestamp2tm(*timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("timestamp out of range")));

					tz = P_DetermineTimeZoneOffset(tm, session_timezone);

					if (tm2ptimestamp(tm, fsec, &tz, &timestamptz) != 0)
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
					result = (timestamptz - SetEpochPTimestamp()) / 1000000.0;
#else
					result = timestamptz - SetEpochPTimestamp();
#endif
					break;
				}
			case DTK_DOW:
			case DTK_ISODOW:
				if (ptimestamp2tm(*timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
				result = p2day(date2p(tm->tm_year, tm->tm_mon, tm->tm_mday));
				if (val == DTK_ISODOW && result == 0)
					result = 7;
				break;

			case DTK_DOY:
				if (ptimestamp2tm(*timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
				result = (date2p(tm->tm_year, tm->tm_mon, tm->tm_mday)
						  - date2p(tm->tm_year, 1, 1) + 1);
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp units \"%s\" not supported",
								lowunits)));
				result = 0;
		}

	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("timestamp units \"%s\" not recognized", lowunits)));
		result = 0;
	}

	PG_RETURN_FLOAT8(result);
}
/* ptimestamptz_part()
 * Extract specified field from timestamp with time zone.
 */
PG_FUNCTION_INFO_V1(ptimestamptz_part);
Datum
ptimestamptz_part(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_P(0);
	PTimestampTz *timestamp = PG_GETARG_PTIMESTAMPTZ(1);
	float8		result;
	int			tz;
	int			type,
				val;
	char	   *lowunits;
	double		dummy;
	fsec_t		fsec;
	char	   *tzn;
	struct pg_tm tt,
			   *tm = &tt;

	if (PTIMESTAMP_NOT_FINITE(*timestamp))
	{
		result = 0;
		PG_RETURN_FLOAT8(result);
	}

	lowunits = downcase_truncate_identifier(VARDATA(units),
											VARSIZE(units) - VARHDRSZ,
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (type == UNITS)
	{
		if (ptimestamp2tm(*timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_TZ:
				result = -tz;
				break;

			case DTK_TZ_MINUTE:
				result = -tz;
				result /= MINS_PER_HOUR;
				FMODULO(result, dummy, (double) MINS_PER_HOUR);
				break;

			case DTK_TZ_HOUR:
				dummy = -tz;
				FMODULO(dummy, result, (double) SECS_PER_HOUR);
				break;

			case DTK_MICROSEC:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec * 1000000.0 + fsec;
#else
				result = (tm->tm_sec + fsec) * 1000000;
#endif
				break;

			case DTK_MILLISEC:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec * 1000.0 + fsec / 1000.0;
#else
				result = (tm->tm_sec + fsec) * 1000;
#endif
				break;

			case DTK_SECOND:
#ifdef HAVE_INT64_TIMESTAMP
				result = tm->tm_sec + fsec / 1000000.0;
#else
				result = tm->tm_sec + fsec;
#endif
				break;

			case DTK_MINUTE:
				result = tm->tm_min;
				break;

			case DTK_HOUR:
				result = tm->tm_hour;
				break;

			case DTK_DAY:
				result = tm->tm_mday;
				break;

			case DTK_MONTH:
				result = tm->tm_mon;
				break;

			case DTK_QUARTER:
				result = (tm->tm_mon - 1) / 3 + 1;
				break;

			case DTK_WEEK:
				result = (float8) date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);
				break;

			case DTK_YEAR:
				if (tm->tm_year > 0)
					result = tm->tm_year;
				else
					// there is no year 0, just 1 BC and 1 AD 
					result = tm->tm_year - 1;
				break;

			case DTK_DECADE:
				// see comments in ptimestamp_part 
				if (tm->tm_year > 0)
					result = tm->tm_year / 10;
				else
					result = -((8 - (tm->tm_year - 1)) / 10);
				break;

			case DTK_CENTURY:
				// see comments in ptimestamp_part 
				if (tm->tm_year > 0)
					result = (tm->tm_year + 99) / 100;
				else
					result = -((99 - (tm->tm_year - 1)) / 100);
				break;

			case DTK_MILLENNIUM:
				// see comments in ptimestamp_part 
				if (tm->tm_year > 0)
					result = (tm->tm_year + 999) / 1000;
				else
					result = -((999 - (tm->tm_year - 1)) / 1000);
				break;

			case DTK_JULIAN:
				result = date2p(tm->tm_year, tm->tm_mon, tm->tm_mday);
#ifdef HAVE_INT64_TIMESTAMP
				result += ((((tm->tm_hour * MINS_PER_HOUR) + tm->tm_min) * SECS_PER_MINUTE) +
					tm->tm_sec + (fsec / 1000000.0)) / (double) SECS_PER_DAY;
#else
				result += ((((tm->tm_hour * MINS_PER_HOUR) + tm->tm_min) * SECS_PER_MINUTE) +
						   tm->tm_sec + fsec) / (double) SECS_PER_DAY;
#endif
				break;

			case DTK_ISOYEAR:
				result = date2isoyear(tm->tm_year, tm->tm_mon, tm->tm_mday);
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("timestamp with time zone units \"%s\" not supported",
					   lowunits)));
				result = 0;
		}

	}
	else if (type == RESERV)
	{
		switch (val)
		{
			case DTK_EPOCH:
#ifdef HAVE_INT64_TIMESTAMP
				result = (*timestamp - SetEpochPTimestamp()) / 1000000.0;
#else
				result = *timestamp - SetEpochPTimestamp();
#endif
				break;

			case DTK_DOW:
			case DTK_ISODOW:
				if (ptimestamp2tm(*timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
				result = p2day(date2p(tm->tm_year, tm->tm_mon, tm->tm_mday));
				if (val == DTK_ISODOW && result == 0)
					result = 7;
				break;

			case DTK_DOY:
				if (ptimestamp2tm(*timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
				result = (date2p(tm->tm_year, tm->tm_mon, tm->tm_mday)
						  - date2p(tm->tm_year, 1, 1) + 1);
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("timestamp with time zone units \"%s\" not supported",
					   lowunits)));
				result = 0;
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			   errmsg("timestamp with time zone units \"%s\" not recognized",
					  lowunits)));

		result = 0;
	}

	PG_RETURN_FLOAT8(result);
}

/*	ptimestamp_zone()
 *	Encode timestamp type with specified time zone.
 *	This function is just ptimestamp2ptimestamptz() except instead of
 *	shifting to the global timezone, we shift to the specified timezone.
 *	This is different from the other AT TIME ZONE cases because instead
 *	of shifting to a _to_ a new time zone, it sets the time to _be_ the
 *	specified timezone.
 */
PG_FUNCTION_INFO_V1(ptimestamp_zone);
Datum
ptimestamp_zone(PG_FUNCTION_ARGS)
{
	text	   *zone = PG_GETARG_TEXT_P(0);
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(1);
	PTimestampTz *result;
	int			tz;
	char		tzname[TZ_STRLEN_MAX + 1];
	int			len;
	char	   *lowzone;
	int			type,
				val;
	pg_tz	   *tzp;

	result = (PTimestampTz *) palloc(sizeof(PTimestampTz));
	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_PTIMESTAMPTZ(timestamp);
	/*
	 * Look up the requested timezone.  First we look in the date token table
	 * (to handle cases like "EST"), and if that fails, we look in the
	 * timezone database (to handle cases like "America/New_York").  (This
	 * matches the order in which timestamp input checks the cases; it's
	 * important because the timezone database unwisely uses a few zone names
	 * that are identical to offset abbreviations.)
	 */
	lowzone = downcase_truncate_identifier(VARDATA(zone),
										   VARSIZE(zone) - VARHDRSZ,
										   false);
	type = DecodeSpecial(0, lowzone, &val);

	if (type == TZ || type == DTZ)
	{
		tz = -(val * 60);
		*result = dt2local(*timestamp, tz);
	}
	else
	{
		len = Min(VARSIZE(zone) - VARHDRSZ, TZ_STRLEN_MAX);
		memcpy(tzname, VARDATA(zone), len);
		tzname[len] = '\0';
		tzp = pg_tzset(tzname);
		if (tzp)
		{
//			 Apply the timezone change 
			struct pg_tm tm;
			fsec_t		fsec;

			if (ptimestamp2tm(*timestamp, NULL, &tm, &fsec, NULL, tzp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
			tz = P_DetermineTimeZoneOffset(&tm, tzp);
			if (tm2ptimestamp(&tm, fsec, &tz, result) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not convert to time zone \"%s\"",
								tzname)));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("time zone \"%s\" not recognized", tzname)));
			*result = 0;			
		}
	}

	PG_RETURN_PTIMESTAMPTZ(result);
}
/* ptimestamp_izone()
 * Encode timestamp type with specified time interval as time zone.
 */
PG_FUNCTION_INFO_V1(ptimestamp_izone);
Datum
ptimestamp_izone(PG_FUNCTION_ARGS)
{
	Interval   *zone = PG_GETARG_INTERVAL_P(0);
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(1);
	PTimestampTz *result;
	int			tz;

	result = (PTimestampTz *) palloc(sizeof(PTimestampTz));
	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_PTIMESTAMPTZ(timestamp);

	if (zone->month != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval time zone \"%s\" must not specify month",
						DatumGetCString(DirectFunctionCall1(interval_out,
												  PointerGetDatum(zone))))));

#ifdef HAVE_INT64_TIMESTAMP
	tz = zone->time / USECS_PER_SEC;
#else
	tz = zone->time;
#endif

	*result = dt2local(*timestamp, tz);

	PG_RETURN_PTIMESTAMPTZ(result);
}	
/* ptimestamp_ptimestamptz()
 * Convert local timestamp to timestamp at GMT
 */
PG_FUNCTION_INFO_V1(ptimestamp_ptimestamptz);
Datum
ptimestamp_ptimestamptz(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(0);
	PTimestampTz *result;
	
	result = (PTimestampTz *) palloc(sizeof(PTimestampTz));
	*result = ptimestamp2ptimestamptz(*timestamp);
	PG_RETURN_PTIMESTAMPTZ(result);
}

static PTimestampTz
ptimestamp2ptimestamptz(PTimestamp timestamp)
{
	PTimestampTz result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	int			tz;

	if (PTIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		if (ptimestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		tz = P_DetermineTimeZoneOffset(tm, session_timezone);

		if (tm2ptimestamp(tm, fsec, &tz, &result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}

	return result;
}
/* ptimestamptz_ptimestamp()
 * Convert timestamp at GMT to local timestamp
 */
PG_FUNCTION_INFO_V1(ptimestamptz_ptimestamp);
Datum
ptimestamptz_ptimestamp(PG_FUNCTION_ARGS)
{
	PTimestampTz *timestamp = PG_GETARG_PTIMESTAMPTZ(0);
	PTimestamp	*result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	char	   *tzn;
	int			tz;

	result = (PTimestamp *) palloc(sizeof(PTimestamp));
	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		*result = *timestamp;
	else
	{
		if (ptimestamp2tm(*timestamp, &tz, tm, &fsec, &tzn, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
		if (tm2ptimestamp(tm, fsec, NULL, result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}
	PG_RETURN_PTIMESTAMP(result);
}
/* timestamp_ptimestamp()
 * Convert timestamp to ptimestamp data type.
 */
PG_FUNCTION_INFO_V1(timestamp_ptimestamp);
Datum
timestamp_ptimestamp(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	PTimestamp	*result;
	struct pg_tm tt,
			   *tm = &tt, ptm;
	fsec_t		fsec;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_NULL();

	if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	
	date2pdate(*tm, &ptm);
	result = (PTimestamp *) palloc(sizeof(PTimestamp));
	tm2ptimestamp(&ptm, fsec, NULL, result);

	PG_RETURN_PTIMESTAMP(result);
}

/* ptimestamp_timestamp()
 * Convert ptimestamp to timestamp data type.
 */
PG_FUNCTION_INFO_V1(ptimestamp_timestamp);
Datum
ptimestamp_timestamp(PG_FUNCTION_ARGS)
{
	PTimestamp	*timestamp = PG_GETARG_PTIMESTAMP(0);
	Timestamp	result;
	struct pg_tm tm, ptm;
	fsec_t		fsec;

	if (TIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_NULL();

	if (ptimestamp2tm(*timestamp, NULL, &ptm, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	
	pdate2date(ptm, &tm);

	if (tm2timestamp(&tm, fsec, NULL, &result) != 0)
			ereport(ERROR,
			(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMP(result);
}


/* ptimestamptz_zone()
 * Evaluate timestamp with time zone type at the specified time zone.
 * Returns a timestamp without time zone.
 */
PG_FUNCTION_INFO_V1(ptimestamptz_zone);
Datum
ptimestamptz_zone(PG_FUNCTION_ARGS)
{
	text	   *zone = PG_GETARG_TEXT_P(0);
	PTimestampTz *timestamp = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestamp	*result;
	int			tz;
	char		tzname[TZ_STRLEN_MAX + 1];
	int			len;
	char	   *lowzone;
	int			type,
				val;
	pg_tz	   *tzp;

	result = (PTimestamp *) palloc(sizeof(PTimestamp));
	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_PTIMESTAMP(timestamp);

	lowzone = downcase_truncate_identifier(VARDATA(zone),
										   VARSIZE(zone) - VARHDRSZ,
										   false);
	type = DecodeSpecial(0, lowzone, &val);

	if (type == TZ || type == DTZ)
	{
		tz = val * 60;
		*result = dt2local(*timestamp, tz);
	}
	else
	{
		len = Min(VARSIZE(zone) - VARHDRSZ, TZ_STRLEN_MAX);
		memcpy(tzname, VARDATA(zone), len);
		tzname[len] = '\0';
		tzp = pg_tzset(tzname);
		if (tzp)
		{
			struct pg_tm tm;
			fsec_t		fsec;

			if (ptimestamp2tm(*timestamp, &tz, &tm, &fsec, NULL, tzp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
			if (tm2ptimestamp(&tm, fsec, NULL, result) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not convert to time zone \"%s\"",
								tzname)));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("time zone \"%s\" not recognized", tzname)));
			*result = 0;			
		}
	}

	PG_RETURN_PTIMESTAMP(result);
}
/* ptimestamptz_izone()
 * Encode timestamp with time zone type with specified time interval as time zone.
 * Returns a timestamp without time zone.
 */
PG_FUNCTION_INFO_V1(ptimestamptz_izone);
Datum
ptimestamptz_izone(PG_FUNCTION_ARGS)
{
	Interval   *zone = PG_GETARG_INTERVAL_P(0);
	PTimestampTz *timestamp = PG_GETARG_PTIMESTAMPTZ(1);
	PTimestamp	*result;
	int			tz;

	result = (PTimestamp *) palloc(sizeof(PTimestamp));
	if (PTIMESTAMP_NOT_FINITE(*timestamp))
		PG_RETURN_PTIMESTAMP(timestamp);

	if (zone->month != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval time zone \"%s\" must not specify month",
						DatumGetCString(DirectFunctionCall1(interval_out,
												  PointerGetDatum(zone))))));

#ifdef HAVE_INT64_TIMESTAMP
	tz = -(zone->time / USECS_PER_SEC);
#else
	tz = -zone->time;
#endif

	*result = dt2local(*timestamp, tz);

	PG_RETURN_PTIMESTAMP(result);
}



