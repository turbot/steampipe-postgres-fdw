
#include "postgres.h"
#include "common.h"
#include "utils/inet.h"
#include "utils/timestamp.h"

char *datumString(Datum datum, ConversionInfo *cinfo) {
    if (datum == 0) {
        return "?";
    }
    return TextDatumGetCString(datum);
}

inet *datumInet(Datum datum, ConversionInfo *cinfo) {
    if (datum == 0) {
        return (inet *)0;
    }
    return DatumGetInetPP(datum);
}

int64 datumInt64(Datum datum, ConversionInfo *cinfo) {
    return DatumGetInt64(datum);
}

double datumDouble(Datum datum, ConversionInfo *cinfo) {
    return DatumGetFloat4(datum);
}

bool datumBool(Datum datum, ConversionInfo *cinfo) {
    return DatumGetBool(datum);
}

Timestamp datumDate(Datum datum, ConversionInfo *cinfo) {
	datum = DirectFunctionCall1(date_timestamp, datum);
    return DatumGetInt64(datum);
}

Timestamp datumTimestamp(Datum datum, ConversionInfo *cinfo) {
    return DatumGetTimestamp(datum);
}