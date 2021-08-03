
#include "postgres.h"
#include "common.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "nodes/extensible.h"
#include "nodes/pg_list.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "storage/ipc.h"
#include "utils/inet.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

extern char **environ;

// Macro expansions
static inline FormData_pg_attribute* fdw_tupleDescAttr(TupleDesc tupdesc, int i) { return TupleDescAttr(tupdesc, i); }
static inline TupleDescData* fdw_relationGetDescr(Relation relation) { return RelationGetDescr(relation); }
static inline Oid fdw_relationGetNamespace(Relation relation) { return RelationGetNamespace(relation); }

static inline void fdw_errorReport(int level, int code, char *msg) {  ereport(level, (errcode(code), errmsg("%s", msg)));  }
static inline void fdw_errorReportWithHint(int level, int code, char *msg, char *hint) { ereport(level, (errcode(code), errmsg("%s", msg), errhint("%s", hint))); }

static inline HeapTuple fdw_searchSysCache1Oid(Datum key1) { return SearchSysCache1(TYPEOID, key1); }
static inline HeapTuple fdw_searchSysCache1(Oid id, Datum key1) {	return SearchSysCache1(id, key1); }
static inline Datum fdw_objectIdGetDatum(Oid id){ return ObjectIdGetDatum(id); }
static inline bool fdw_heapTupleIsValid(HeapTuple tuple){ return HeapTupleIsValid(tuple); }
static inline void *fdw_getStruct(HeapTuple tuple) { return GETSTRUCT(tuple); }

static inline NodeTag fdw_nodeTag(Expr *node) { return nodeTag(node); }

static inline Datum fdw_boolGetDatum(bool b) { PG_RETURN_BOOL(b); }
static inline Datum fdw_cStringGetDatum(const char *str) { PG_RETURN_TEXT_P(CStringGetTextDatum(str)); }
static inline Datum fdw_jsonbGetDatum(const char *str)   { PG_RETURN_JSONB_P(DirectFunctionCall1(jsonb_in, CStringGetDatum(str))); }
static inline Datum fdw_numericGetDatum(int64_t num) { PG_RETURN_INT64(Int64GetDatum(num)); }
static inline Datum fdw_floatGetDatum(double num) { PG_RETURN_FLOAT8(Float8GetDatum(num)); }
static inline Datum fdw_pointerGetDatum(void* num) { PG_RETURN_DATUM(PointerGetDatum(num)); }





static inline void fdw_saveTuple(Datum *data, bool *isnull, ScanState *state) {
  HeapTuple tuple = heap_form_tuple(state->ss_currentRelation->rd_att, data, isnull);
  ExecStoreHeapTuple(tuple, state->ss_ScanTupleSlot, false);
}
static inline ArrayType *fdw_datumGetArrayTypeP(Datum datum) { return ((ArrayType *) PG_DETOAST_DATUM(datum));}
static inline char* fdw_datumGetString(Datum datum) { return text_to_cstring((text *) DatumGetPointer(datum));}


// Helpers
List * extractColumns(List *reltargetlist, List *restrictinfolist);
FdwExecState *initializeExecState(void *internalstate);
FdwExecState *initializeExecState(void *internalstate);

static inline ConversionInfo* getConversionInfo(ConversionInfo **cinfos, int i) { return cinfos[i]; }
static inline char* valueString(Value *v) { return (((Value *)(v))->val.str); }
static inline char** incStringPointer(char **ptr) { return ++ptr; }
static inline unsigned char* incUcharPointer(unsigned char *ptr) { return ++ptr; }
static inline unsigned char* ipAddr(inet *i) { return ip_addr(i); }
static inline unsigned char netmaskBits(inet *i) { return ip_bits(i); }
static inline bool isIpV6(inet *i) { return ip_family(i) == PGSQL_AF_INET6; }

// Loop helpers
static inline RangeVar* cellGetRangeVar(ListCell *n) { return (RangeVar*)n->data.ptr_value; }
static inline DefElem* cellGetDef(ListCell *n) { return (DefElem*)n->data.ptr_value; }
static inline Expr* cellGetExpr(ListCell *n) { return (Expr*)n->data.ptr_value; }
static inline Node* cellGetNode(ListCell *n) { return (Node*)n->data.ptr_value; }
static inline Value* cellGetValue(ListCell *n) { return (Value*)n->data.ptr_value; }
static inline Var* cellGetVar(ListCell *n) { return (Var*)n->data.ptr_value; }
static inline OpExpr* cellGetOpExpr(ListCell *n) { return (OpExpr*)n->data.ptr_value; }
static inline ScalarArrayOpExpr* cellGetScalarArrayOpExpr(ListCell *n) { return (ScalarArrayOpExpr*)n->data.ptr_value; }
static inline NullTest* cellGetNullTest(ListCell *n) { return (NullTest*)n->data.ptr_value; }
static inline BooleanTest* cellGetBooleanTest(ListCell *n) { return (BooleanTest*)n->data.ptr_value; }
static inline BoolExpr* cellGetBoolExpr(ListCell *n) { return (BoolExpr*)n->data.ptr_value; }

static inline RestrictInfo* cellGetRestrictInfo(ListCell *n) { return (RestrictInfo*)n->data.ptr_value; }

// logging
char* tagTypeToString(NodeTag type);