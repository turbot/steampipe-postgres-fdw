#include "postgres.h"
#include "access/attnum.h"
#include "access/relscan.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/inet.h"
#include "utils/jsonb.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#endif

#ifndef FDW_COMMON_H
#define FDW_COMMON_H

typedef struct ConversionInfo
{
  char *attrname;
  FmgrInfo *attinfunc;
  FmgrInfo *attoutfunc;
  Oid atttypoid;
  Oid attioparam;
  int32 atttypmod;
  int attnum;
  bool is_array;
  int attndims;
  bool need_quote;
} ConversionInfo;

typedef struct FdwPathData {
    List *deparsed_pathkeys;
    bool canPushdownAllSortFields;
} FdwPathData;

typedef struct FdwPlanState
{
  Oid foreigntableid;
  AttrNumber numattrs;
  int fdw_instance;
  List *target_list;
  int startupCost;
  ConversionInfo **cinfos;
  List *pathkeys; /* list of FdwDeparsedSortGroup) */
  /* For some reason, `baserel->reltarget->width` gets changed
   * outside of our control somewhere between GetForeignPaths and
   * GetForeignPlan, which breaks tests.
   *
   * XXX: This is very crude hack to transfer width, calculated by
   * getRelSize to GetForeignPlan.
   */
  // can all sort fields be pushed down?
  // this is tru if there are NO sort fields, or if ALL sort fields can be pushed down
  // this is used by goFdwBeginForeignScan to decide whether to push down the limit
  bool canPushdownAllSortFields;
  int width;
  // the number of rows to return (limit+offset). -1 means no limit
  int limit;

} FdwPlanState;

typedef struct FdwExecState
{
  /* Information carried from the plan phase. */
  List *target_list;
  Datum *values;
  bool *nulls;
  int numattrs;
  ConversionInfo **cinfos;
  /* Common buffer to avoid repeated allocations */
  StringInfo buffer;
  AttrNumber rowidAttno;
  char *rowidAttrName;
  List *pathkeys; /* list of FdwDeparsedSortGroup) */
  // the number of rows to return (limit+offset). -1 means no limit
  int limit;
  // can all sort fields be pushed down?
  // this is tru if there are NO sort fields, or if ALL sort fields can be pushed down
  // this is used by goFdwBeginForeignScan to decide whether to push down the limit
  bool canPushdownAllSortFields;
} FdwExecState;

typedef struct FdwDeparsedSortGroup
{
  Name attname;
  int attnum;
  bool reversed;
  bool nulls_first;
  Name collate;
  PathKey *key;
} FdwDeparsedSortGroup;

static inline FdwDeparsedSortGroup *cellGetFdwDeparsedSortGroup(ListCell *n) { return (FdwDeparsedSortGroup *)n->ptr_value; }

// datum.c
char *datumString(Datum datum, ConversionInfo *cinfo);
int64 datumInt16(Datum datum, ConversionInfo *cinfo);
int64 datumInt32(Datum datum, ConversionInfo *cinfo);
int64 datumInt64(Datum datum, ConversionInfo *cinfo);
inet *datumInet(Datum datum, ConversionInfo *cinfo);
inet *datumCIDR(Datum datum, ConversionInfo *cinfo);
double datumFloat4(Datum datum, ConversionInfo *cinfo);
double datumFloat8(Datum datum, ConversionInfo *cinfo);
bool datumBool(Datum datum, ConversionInfo *cinfo);
Jsonb *datumJsonb(Datum datum, ConversionInfo *cinfo);
Timestamp datumDate(Datum datum, ConversionInfo *cinfo);
Timestamp datumTimestamp(Datum datum, ConversionInfo *cinfo);

// query.c
List *extractColumns(List *reltargetlist, List *restrictinfolist);
void initConversioninfo(ConversionInfo **cinfo, AttInMetadata *attinmeta);
#if PG_VERSION_NUM >= 150000
String *colnameFromVar(Var *var, PlannerInfo *root, FdwPlanState *state);
#else
Value *colnameFromVar(Var *var, PlannerInfo *root, FdwPlanState *state);
#endif
bool computeDeparsedSortGroup(List *deparsed, FdwPlanState *planstate, List **apply_pathkeys, List **deparsed_pathkeys);
List *findPaths(PlannerInfo *root, RelOptInfo *baserel, List *possiblePaths, int startupCost, FdwPlanState *state, List *apply_pathkeys, List *deparsed_pathkeys);
List *deparse_sortgroup(PlannerInfo *root, Oid foreigntableid, RelOptInfo *rel);
List *serializeDeparsedSortGroup(List *pathkeys);
List *deserializeDeparsedSortGroup(List *items);
OpExpr *canonicalOpExpr(OpExpr *opExpr, Relids base_relids);
ScalarArrayOpExpr *canonicalScalarArrayOpExpr(ScalarArrayOpExpr *opExpr, Relids base_relids);
char *getOperatorString(Oid opoid);
#endif // FDW_COMMON_H
