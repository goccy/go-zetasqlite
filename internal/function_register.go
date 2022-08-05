package internal

import (
	"fmt"
	"sync"

	"github.com/goccy/go-zetasql/types"
	"github.com/mattn/go-sqlite3"
)

var normalFuncs = []*FuncInfo{
	{
		Name:        "add",
		BindFunc:    bindAdd,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE, types.DATE},
	},
	{
		Name:        "subtract",
		BindFunc:    bindSub,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE, types.DATE},
	},
	{
		Name:        "multiply",
		BindFunc:    bindMul,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "divide",
		BindFunc:    bindOpDiv,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "equal",
		BindFunc:    bindEqual,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "not_equal",
		BindFunc:    bindNotEqual,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "greater",
		BindFunc:    bindGreater,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "greater_or_equal",
		BindFunc:    bindGreaterOrEqual,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "less",
		BindFunc:    bindLess,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "less_or_equal",
		BindFunc:    bindLessOrEqual,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "bitwise_not",
		BindFunc:    bindBitNot,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "bitwise_left_shift",
		BindFunc:    bindBitLeftShift,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "bitwise_right_shift",
		BindFunc:    bindBitRightShift,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "bitwise_and",
		BindFunc:    bindBitAnd,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "bitwise_or",
		BindFunc:    bindBitOr,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "bitwise_xor",
		BindFunc:    bindBitXor,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "in_array",
		BindFunc:    bindInArray,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:     "get_struct_field",
		BindFunc: bindStructField,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.BOOL,
			types.STRING, types.ARRAY, types.STRUCT,
		},
	},
	{
		Name:     "array_at_offset",
		BindFunc: bindArrayAtOffset,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING,
			types.BOOL, types.STRUCT,
		},
	},
	{
		Name:     "array_at_ordinal",
		BindFunc: bindArrayAtOrdinal,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING,
			types.BOOL, types.STRUCT,
		},
	},
	{
		Name:     "safe_array_at_offset",
		BindFunc: bindSafeArrayAtOffset,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING,
			types.BOOL, types.STRUCT,
		},
	},
	{
		Name:     "safe_array_at_ordinal",
		BindFunc: bindSafeArrayAtOrdinal,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING,
			types.BOOL, types.STRUCT,
		},
	},
	{
		Name:        "is_distinct_from",
		BindFunc:    bindIsDistinctFrom,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "is_not_distinct_from",
		BindFunc:    bindIsNotDistinctFrom,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},

	// date functions
	{
		Name:        "current_date",
		BindFunc:    bindCurrentDate,
		ReturnTypes: []types.TypeKind{types.DATE},
	},
	{
		Name:        "extract",
		BindFunc:    bindExtract,
		ReturnTypes: []types.TypeKind{types.INT64, types.DATE, types.DATETIME, types.TIME},
	},
	{
		Name:        "date",
		BindFunc:    bindDate,
		ReturnTypes: []types.TypeKind{types.DATE},
	},
	{
		Name:        "date_add",
		BindFunc:    bindDateAdd,
		ReturnTypes: []types.TypeKind{types.DATE},
	},
	{
		Name:        "date_sub",
		BindFunc:    bindDateSub,
		ReturnTypes: []types.TypeKind{types.DATE},
	},
	{
		Name:        "date_diff",
		BindFunc:    bindDateDiff,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "date_trunc",
		BindFunc:    bindDateTrunc,
		ReturnTypes: []types.TypeKind{types.DATE},
	},
	{
		Name:        "date_from_unix_date",
		BindFunc:    bindDateFromUnixDate,
		ReturnTypes: []types.TypeKind{types.DATE},
	},
	{
		Name:        "last_day",
		BindFunc:    bindLastDay,
		ReturnTypes: []types.TypeKind{types.DATE, types.DATETIME},
	},
	{
		Name:        "parse_date",
		BindFunc:    bindParseDate,
		ReturnTypes: []types.TypeKind{types.DATE},
	},
	{
		Name:        "unix_date",
		BindFunc:    bindUnixDate,
		ReturnTypes: []types.TypeKind{types.INT64},
	},

	// datetime functions
	{
		Name:        "current_datetime",
		BindFunc:    bindCurrentDatetime,
		ReturnTypes: []types.TypeKind{types.DATETIME},
	},
	{
		Name:        "datetime",
		BindFunc:    bindDatetime,
		ReturnTypes: []types.TypeKind{types.DATETIME},
	},
	{
		Name:        "datetime_add",
		BindFunc:    bindDatetimeAdd,
		ReturnTypes: []types.TypeKind{types.DATETIME},
	},
	{
		Name:        "datetime_sub",
		BindFunc:    bindDatetimeSub,
		ReturnTypes: []types.TypeKind{types.DATETIME},
	},
	{
		Name:        "datetime_diff",
		BindFunc:    bindDatetimeDiff,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "datetime_trunc",
		BindFunc:    bindDatetimeTrunc,
		ReturnTypes: []types.TypeKind{types.DATETIME},
	},
	{
		Name:        "parse_datetime",
		BindFunc:    bindParseDatetime,
		ReturnTypes: []types.TypeKind{types.DATETIME},
	},

	// time functions
	{
		Name:        "current_time",
		BindFunc:    bindCurrentTime,
		ReturnTypes: []types.TypeKind{types.TIME},
	},
	{
		Name:        "time",
		BindFunc:    bindTime,
		ReturnTypes: []types.TypeKind{types.TIME},
	},
	{
		Name:        "time_add",
		BindFunc:    bindTimeAdd,
		ReturnTypes: []types.TypeKind{types.TIME},
	},
	{
		Name:        "time_sub",
		BindFunc:    bindTimeSub,
		ReturnTypes: []types.TypeKind{types.TIME},
	},
	{
		Name:        "time_diff",
		BindFunc:    bindTimeDiff,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "time_trunc",
		BindFunc:    bindTimeTrunc,
		ReturnTypes: []types.TypeKind{types.TIME},
	},
	{
		Name:        "parse_time",
		BindFunc:    bindParseTime,
		ReturnTypes: []types.TypeKind{types.TIME},
	},

	// timestamp functions
	{
		Name:        "current_timestamp",
		BindFunc:    bindCurrentTimestamp,
		ReturnTypes: []types.TypeKind{types.TIMESTAMP},
	},
	{
		Name:        "string",
		BindFunc:    bindString,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "timestamp",
		BindFunc:    bindTimestamp,
		ReturnTypes: []types.TypeKind{types.TIMESTAMP},
	},
	{
		Name:        "timestamp_add",
		BindFunc:    bindTimestampAdd,
		ReturnTypes: []types.TypeKind{types.TIMESTAMP},
	},
	{
		Name:        "timestamp_sub",
		BindFunc:    bindTimestampSub,
		ReturnTypes: []types.TypeKind{types.TIMESTAMP},
	},
	{
		Name:        "timestamp_diff",
		BindFunc:    bindTimestampDiff,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "timestamp_trunc",
		BindFunc:    bindTimestampTrunc,
		ReturnTypes: []types.TypeKind{types.TIMESTAMP},
	},
	{
		Name:        "parse_timestamp",
		BindFunc:    bindParseTimestamp,
		ReturnTypes: []types.TypeKind{types.TIMESTAMP},
	},
	{
		Name:        "timestamp_seconds",
		BindFunc:    bindTimestampSeconds,
		ReturnTypes: []types.TypeKind{types.TIMESTAMP},
	},
	{
		Name:        "timestamp_millis",
		BindFunc:    bindTimestampMillis,
		ReturnTypes: []types.TypeKind{types.TIMESTAMP},
	},
	{
		Name:        "timestamp_micros",
		BindFunc:    bindTimestampMicros,
		ReturnTypes: []types.TypeKind{types.TIMESTAMP},
	},
	{
		Name:        "unix_seconds",
		BindFunc:    bindUnixSeconds,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "unix_millis",
		BindFunc:    bindUnixMillis,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "unix_micros",
		BindFunc:    bindUnixMicros,
		ReturnTypes: []types.TypeKind{types.INT64},
	},

	{
		Name:        "concat",
		BindFunc:    bindConcat,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "like",
		BindFunc:    bindLike,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "between",
		BindFunc:    bindBetween,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "in",
		BindFunc:    bindIn,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "is_null",
		BindFunc:    bindIsNull,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "is_true",
		BindFunc:    bindIsTrue,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "is_false",
		BindFunc:    bindIsFalse,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "not",
		BindFunc:    bindNot,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "and",
		BindFunc:    bindAnd,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "or",
		BindFunc:    bindOr,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:     "case_with_value",
		BindFunc: bindCaseWithValue,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING, types.BOOL,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
		},
	},
	{
		Name:     "case_no_value",
		BindFunc: bindCaseNoValue,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING, types.BOOL,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
		},
	},
	{
		Name:     "coalesce",
		BindFunc: bindCoalesce,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING, types.BOOL,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
		},
	},
	{
		Name:     "if",
		BindFunc: bindIf,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING, types.BOOL,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
			types.ARRAY, types.STRUCT,
		},
	},
	{
		Name:     "ifnull",
		BindFunc: bindIfNull,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING, types.BOOL,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
			types.ARRAY, types.STRUCT,
		},
	},
	{
		Name:     "nullif",
		BindFunc: bindNullIf,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING, types.BOOL,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
			types.ARRAY, types.STRUCT,
		},
	},
	{
		Name:        "length",
		BindFunc:    bindLength,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:     "cast",
		BindFunc: bindCast,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.BOOL, types.STRING,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
			types.ARRAY, types.STRUCT,
		},
	},
	{
		Name:        "castbool",
		BindFunc:    bindCastBoolString,
		ReturnTypes: []types.TypeKind{types.STRING},
	},

	{
		Name:     "safe_cast",
		BindFunc: bindCast,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.BOOL, types.STRING,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
			types.ARRAY, types.STRUCT,
		},
	},

	// hash functions
	{
		Name:        "farm_fingerprint",
		BindFunc:    bindFarmFingerprint,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "md5",
		BindFunc:    bindMD5,
		ReturnTypes: []types.TypeKind{types.BYTES},
	},
	{
		Name:        "sha1",
		BindFunc:    bindSha1,
		ReturnTypes: []types.TypeKind{types.BYTES},
	},
	{
		Name:        "sha256",
		BindFunc:    bindSha256,
		ReturnTypes: []types.TypeKind{types.BYTES},
	},
	{
		Name:        "sha512",
		BindFunc:    bindSha512,
		ReturnTypes: []types.TypeKind{types.BYTES},
	},

	// string functions
	{
		Name:        "format",
		BindFunc:    bindFormat,
		ReturnTypes: []types.TypeKind{types.STRING},
	},

	// math functions

	{
		Name:        "abs",
		BindFunc:    bindAbs,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "sign",
		BindFunc:    bindSign,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "is_inf",
		BindFunc:    bindIsInf,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "is_nan",
		BindFunc:    bindIsNaN,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "ieee_divide",
		BindFunc:    bindIEEEDivide,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "rand",
		BindFunc:    bindRand,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "sqrt",
		BindFunc:    bindSqrt,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "pow",
		BindFunc:    bindPow,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "power",
		BindFunc:    bindPow,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "exp",
		BindFunc:    bindExp,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "ln",
		BindFunc:    bindLn,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "log",
		BindFunc:    bindLog,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "log10",
		BindFunc:    bindLog10,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "greatest",
		BindFunc:    bindGreatest,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "least",
		BindFunc:    bindLeast,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "div",
		BindFunc:    bindDiv,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "safe_divide",
		BindFunc:    bindSafeDivide,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "safe_multiply",
		BindFunc:    bindSafeMultiply,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "safe_negate",
		BindFunc:    bindSafeNegate,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "safe_add",
		BindFunc:    bindSafeAdd,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "safe_subtract",
		BindFunc:    bindSafeSubtract,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "mod",
		BindFunc:    bindMod,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "round",
		BindFunc:    bindRound,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "trunc",
		BindFunc:    bindTrunc,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "ceil",
		BindFunc:    bindCeil,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "ceiling",
		BindFunc:    bindCeil,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "floor",
		BindFunc:    bindFloor,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "cos",
		BindFunc:    bindCos,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "cosh",
		BindFunc:    bindCosh,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "acos",
		BindFunc:    bindAcos,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "acosh",
		BindFunc:    bindAcosh,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "sin",
		BindFunc:    bindSin,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "sinh",
		BindFunc:    bindSinh,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "asin",
		BindFunc:    bindAsin,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "asinh",
		BindFunc:    bindAsinh,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "tan",
		BindFunc:    bindTan,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "tanh",
		BindFunc:    bindTanh,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "atan",
		BindFunc:    bindAtan,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "atanh",
		BindFunc:    bindAtanh,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "atan2",
		BindFunc:    bindAtan2,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "range_bucket",
		BindFunc:    bindRangeBucket,
		ReturnTypes: []types.TypeKind{types.INT64},
	},

	// encoded array to json array helper func
	{
		Name:        "decode_array",
		BindFunc:    bindDecodeArray,
		ReturnTypes: []types.TypeKind{types.STRING},
	},

	// array functions
	{
		Name:        "array_concat",
		BindFunc:    bindArrayConcat,
		ReturnTypes: []types.TypeKind{types.ARRAY},
	},
	{
		Name:        "array_length",
		BindFunc:    bindArrayLength,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "array_to_string",
		BindFunc:    bindArrayToString,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "generate_array",
		BindFunc:    bindGenerateArray,
		ReturnTypes: []types.TypeKind{types.ARRAY},
	},
	{
		Name:        "generate_date_array",
		BindFunc:    bindGenerateDateArray,
		ReturnTypes: []types.TypeKind{types.ARRAY},
	},
	{
		Name:        "generate_timestamp_array",
		BindFunc:    bindGenerateTimestampArray,
		ReturnTypes: []types.TypeKind{types.ARRAY},
	},
	{
		Name:        "array_reverse",
		BindFunc:    bindArrayReverse,
		ReturnTypes: []types.TypeKind{types.ARRAY},
	},

	{
		Name:        "make_struct",
		BindFunc:    bindMakeStruct,
		ReturnTypes: []types.TypeKind{types.STRUCT},
	},

	// aggregate option funcs
	{
		Name:        "distinct",
		BindFunc:    bindDistinct,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "limit",
		BindFunc:    bindLimit,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "order_by",
		BindFunc:    bindOrderBy,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "ignore_nulls",
		BindFunc:    bindIgnoreNulls,
		ReturnTypes: []types.TypeKind{types.STRING},
	},

	// window option funcs
	{
		Name:        "window_frame_unit",
		BindFunc:    bindWindowFrameUnit,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "window_partition",
		BindFunc:    bindWindowPartition,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "window_boundary_start",
		BindFunc:    bindWindowBoundaryStart,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "window_boundary_end",
		BindFunc:    bindWindowBoundaryEnd,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "window_rowid",
		BindFunc:    bindWindowRowID,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "window_order_by",
		BindFunc:    bindWindowOrderBy,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
}

var aggregateFuncs = []*AggregateFuncInfo{
	{
		Name:        "array_agg",
		BindFunc:    bindArrayAgg,
		ReturnTypes: []types.TypeKind{types.ARRAY},
	},
	{
		Name:        "array_concat_agg",
		BindFunc:    bindArrayConcatAgg,
		ReturnTypes: []types.TypeKind{types.ARRAY},
	},
	{
		Name:        "sum",
		BindFunc:    bindSum,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "avg",
		BindFunc:    bindAvg,
		ReturnTypes: []types.TypeKind{types.DOUBLE},
	},
	{
		Name:        "count",
		BindFunc:    bindCount,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "count_star",
		BindFunc:    bindCountStar,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "bit_and",
		BindFunc:    bindBitAndAgg,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "bit_or",
		BindFunc:    bindBitOrAgg,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "bit_xor",
		BindFunc:    bindBitXorAgg,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "countif",
		BindFunc:    bindCountIf,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "logical_and",
		BindFunc:    bindLogicalAnd,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:        "logical_or",
		BindFunc:    bindLogicalOr,
		ReturnTypes: []types.TypeKind{types.BOOL},
	},
	{
		Name:     "max",
		BindFunc: bindMax,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.BOOL,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
		},
	},
	{
		Name:     "min",
		BindFunc: bindMin,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.BOOL,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
		},
	},
	{
		Name:        "string_agg",
		BindFunc:    bindStringAgg,
		ReturnTypes: []types.TypeKind{types.STRING},
	},
	{
		Name:        "array",
		BindFunc:    bindArray,
		ReturnTypes: []types.TypeKind{types.ARRAY},
	},
}

var windowFuncs = []*WindowFuncInfo{
	{
		Name:        "sum",
		BindFunc:    bindWindowSum,
		ReturnTypes: []types.TypeKind{types.INT64, types.DOUBLE},
	},
	{
		Name:        "count_star",
		BindFunc:    bindWindowCountStar,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "avg",
		BindFunc:    bindWindowAvg,
		ReturnTypes: []types.TypeKind{types.DOUBLE},
	},
	{
		Name:     "first_value",
		BindFunc: bindWindowFirstValue,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING, types.BOOL,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
		},
	},
	{
		Name:     "last_value",
		BindFunc: bindWindowLastValue,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING, types.BOOL,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
		},
	},
	{
		Name:     "lag",
		BindFunc: bindWindowLag,
		ReturnTypes: []types.TypeKind{
			types.INT64, types.DOUBLE, types.STRING, types.BOOL,
			types.DATE, types.DATETIME, types.TIME, types.TIMESTAMP,
		},
	},
	{
		Name:        "rank",
		BindFunc:    bindWindowRank,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "dense_rank",
		BindFunc:    bindWindowDenseRank,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
	{
		Name:        "row_number",
		BindFunc:    bindWindowRowNumber,
		ReturnTypes: []types.TypeKind{types.INT64},
	},
}

type NameAndFunc struct {
	Name string
	Func interface{}
}

var (
	funcMapMu          sync.RWMutex
	registerFuncOnce   sync.Once
	normalFuncMap      = map[string][]*NameAndFunc{}
	aggregateFuncMap   = map[string][]*NameAndFunc{}
	windowFuncMap      = map[string][]*NameAndFunc{}
	currentTimeFuncMap = map[string]struct{}{
		"current_date":      struct{}{},
		"current_datetime":  struct{}{},
		"current_time":      struct{}{},
		"current_timestamp": struct{}{},
	}
)

func RegisterFunctions(conn *sqlite3.SQLiteConn) error {
	funcMapMu.RLock()
	defer funcMapMu.RUnlock()

	var onceErr error
	registerFuncOnce.Do(func() {
		for _, info := range normalFuncs {
			if err := setupNormalFuncMap(info); err != nil {
				onceErr = err
				return
			}
		}
		for _, info := range aggregateFuncs {
			if err := setupAggregateFuncMap(info); err != nil {
				onceErr = err
				return
			}
		}
		for _, info := range windowFuncs {
			if err := setupWindowFuncMap(info); err != nil {
				onceErr = err
				return
			}
		}
	})
	if onceErr != nil {
		return onceErr
	}

	for _, values := range normalFuncMap {
		for _, v := range values {
			if err := conn.RegisterFunc(v.Name, v.Func, true); err != nil {
				return fmt.Errorf("failed to register function %s: %w", v.Name, err)
			}
		}
	}
	for _, values := range aggregateFuncMap {
		for _, v := range values {
			if err := conn.RegisterAggregator(v.Name, v.Func, true); err != nil {
				return fmt.Errorf("failed to register aggregate function %s: %w", v.Name, err)
			}
		}
	}
	for _, values := range windowFuncMap {
		for _, v := range values {
			if err := conn.RegisterAggregator(v.Name, v.Func, true); err != nil {
				return fmt.Errorf("failed to register window function %s: %w", v.Name, err)
			}
		}
	}
	return nil
}

func setupNormalFuncMap(info *FuncInfo) error {
	for _, retType := range info.ReturnTypes {
		var (
			name string
			fn   interface{}
		)
		switch retType {
		case types.INT64:
			name = fmt.Sprintf("zetasqlite_%s_int64", info.Name)
			fn = bindIntFunc(info.BindFunc)
		case types.DOUBLE:
			name = fmt.Sprintf("zetasqlite_%s_double", info.Name)
			fn = bindFloatFunc(info.BindFunc)
		case types.STRING:
			name = fmt.Sprintf("zetasqlite_%s_string", info.Name)
			fn = bindStringFunc(info.BindFunc)
		case types.BYTES:
			name = fmt.Sprintf("zetasqlite_%s_bytes", info.Name)
			fn = bindBytesFunc(info.BindFunc)
		case types.BOOL:
			name = fmt.Sprintf("zetasqlite_%s_bool", info.Name)
			fn = bindBoolFunc(info.BindFunc)
		case types.DATE:
			name = fmt.Sprintf("zetasqlite_%s_date", info.Name)
			fn = bindDateFunc(info.BindFunc)
		case types.DATETIME:
			name = fmt.Sprintf("zetasqlite_%s_datetime", info.Name)
			fn = bindDatetimeFunc(info.BindFunc)
		case types.TIME:
			name = fmt.Sprintf("zetasqlite_%s_time", info.Name)
			fn = bindTimeFunc(info.BindFunc)
		case types.TIMESTAMP:
			name = fmt.Sprintf("zetasqlite_%s_timestamp", info.Name)
			fn = bindTimestampFunc(info.BindFunc)
		case types.ARRAY:
			name = fmt.Sprintf("zetasqlite_%s_array", info.Name)
			fn = bindArrayFunc(info.BindFunc)
		case types.STRUCT:
			name = fmt.Sprintf("zetasqlite_%s_struct", info.Name)
			fn = bindStructFunc(info.BindFunc)
		default:
			return fmt.Errorf("unsupported return type %s for function: %s", retType, info.Name)
		}
		normalFuncMap[info.Name] = append(normalFuncMap[info.Name], &NameAndFunc{
			Name: name,
			Func: fn,
		})
	}
	return nil
}

func setupAggregateFuncMap(info *AggregateFuncInfo) error {
	for _, retType := range info.ReturnTypes {
		var (
			name       string
			aggregator interface{}
		)
		switch retType {
		case types.INT64:
			name = fmt.Sprintf("zetasqlite_%s_int64", info.Name)
			aggregator = bindAggregateIntFunc(info.BindFunc)
		case types.DOUBLE:
			name = fmt.Sprintf("zetasqlite_%s_double", info.Name)
			aggregator = bindAggregateFloatFunc(info.BindFunc)
		case types.STRING:
			name = fmt.Sprintf("zetasqlite_%s_string", info.Name)
			aggregator = bindAggregateStringFunc(info.BindFunc)
		case types.BYTES:
			name = fmt.Sprintf("zetasqlite_%s_bytes", info.Name)
			aggregator = bindAggregateBytesFunc(info.BindFunc)
		case types.BOOL:
			name = fmt.Sprintf("zetasqlite_%s_bool", info.Name)
			aggregator = bindAggregateBoolFunc(info.BindFunc)
		case types.DATE:
			name = fmt.Sprintf("zetasqlite_%s_date", info.Name)
			aggregator = bindAggregateDateFunc(info.BindFunc)
		case types.DATETIME:
			name = fmt.Sprintf("zetasqlite_%s_datetime", info.Name)
			aggregator = bindAggregateDatetimeFunc(info.BindFunc)
		case types.TIME:
			name = fmt.Sprintf("zetasqlite_%s_time", info.Name)
			aggregator = bindAggregateTimeFunc(info.BindFunc)
		case types.TIMESTAMP:
			name = fmt.Sprintf("zetasqlite_%s_timestamp", info.Name)
			aggregator = bindAggregateTimestampFunc(info.BindFunc)
		case types.ARRAY:
			name = fmt.Sprintf("zetasqlite_%s_array", info.Name)
			aggregator = bindAggregateArrayFunc(info.BindFunc)
		case types.STRUCT:
			name = fmt.Sprintf("zetasqlite_%s_struct", info.Name)
			aggregator = bindAggregateStructFunc(info.BindFunc)
		default:
			return fmt.Errorf("unsupported return type %s for aggregate function: %s", retType, info.Name)
		}
		aggregateFuncMap[info.Name] = append(aggregateFuncMap[info.Name], &NameAndFunc{
			Name: name,
			Func: aggregator,
		})
	}
	return nil
}

func setupWindowFuncMap(info *WindowFuncInfo) error {
	for _, retType := range info.ReturnTypes {
		var (
			name       string
			aggregator interface{}
		)
		switch retType {
		case types.INT64:
			name = fmt.Sprintf("zetasqlite_window_%s_int64", info.Name)
			aggregator = bindWindowIntFunc(info.BindFunc)
		case types.DOUBLE:
			name = fmt.Sprintf("zetasqlite_window_%s_double", info.Name)
			aggregator = bindWindowFloatFunc(info.BindFunc)
		case types.STRING:
			name = fmt.Sprintf("zetasqlite_window_%s_string", info.Name)
			aggregator = bindWindowStringFunc(info.BindFunc)
		case types.BYTES:
			name = fmt.Sprintf("zetasqlite_window_%s_bytes", info.Name)
			aggregator = bindWindowBytesFunc(info.BindFunc)
		case types.BOOL:
			name = fmt.Sprintf("zetasqlite_window_%s_bool", info.Name)
			aggregator = bindWindowBoolFunc(info.BindFunc)
		case types.DATE:
			name = fmt.Sprintf("zetasqlite_window_%s_date", info.Name)
			aggregator = bindWindowDateFunc(info.BindFunc)
		case types.DATETIME:
			name = fmt.Sprintf("zetasqlite_window_%s_datetime", info.Name)
			aggregator = bindWindowDatetimeFunc(info.BindFunc)
		case types.TIME:
			name = fmt.Sprintf("zetasqlite_window_%s_time", info.Name)
			aggregator = bindWindowTimeFunc(info.BindFunc)
		case types.TIMESTAMP:
			name = fmt.Sprintf("zetasqlite_window_%s_timestamp", info.Name)
			aggregator = bindWindowTimestampFunc(info.BindFunc)
		case types.ARRAY:
			name = fmt.Sprintf("zetasqlite_window_%s_array", info.Name)
			aggregator = bindWindowArrayFunc(info.BindFunc)
		case types.STRUCT:
			name = fmt.Sprintf("zetasqlite_window_%s_struct", info.Name)
			aggregator = bindWindowStructFunc(info.BindFunc)
		default:
			return fmt.Errorf("unsupported return type %s for window function: %s", retType, info.Name)
		}
		windowFuncMap[info.Name] = append(windowFuncMap[info.Name], &NameAndFunc{
			Name: name,
			Func: aggregator,
		})
	}
	return nil
}
