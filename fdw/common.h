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
	char	   *attrname;
	FmgrInfo   *attinfunc;
	FmgrInfo   *attoutfunc;
	Oid			atttypoid;
	Oid			attioparam;
	int32		atttypmod;
	int			attnum;
	bool		is_array;
	int			attndims;
	bool		need_quote;
}	ConversionInfo;

typedef struct FdwPlanState
{
	Oid			foreigntableid;
	AttrNumber	numattrs;
	int fdw_instance;
	//PyObject   *fdw_instance;
	List	   *target_list;
	List	   *qual_list;
	int			startupCost;
	ConversionInfo **cinfos;
	List	   *pathkeys; /* list of FdwDeparsedSortGroup) */
	/* For some reason, `baserel->reltarget->width` gets changed
	 * outside of our control somewhere between GetForeignPaths and
	 * GetForeignPlan, which breaks tests.
	 *
	 * XXX: This is very crude hack to transfer width, calculated by
	 * getRelSize to GetForeignPlan.
	 */
	int width;
}	FdwPlanState;


typedef struct FdwExecState
{
	/* instance and iterator */
	//PyObject   *fdw_instance;
	//PyObject   *p_iterator;
	/* Information carried from the plan phase. */
	List	   *target_list;
	List	   *qual_list;
	Datum	   *values;
	bool	   *nulls;
	ConversionInfo **cinfos;
	/* Common buffer to avoid repeated allocations */
	StringInfo	buffer;
	AttrNumber	rowidAttno;
	char	   *rowidAttrName;
	List	   *pathkeys; /* list of FdwDeparsedSortGroup) */
}	FdwExecState;

typedef struct FdwBaseQual
{
	AttrNumber	varattno;
	NodeTag		right_type;
	Oid			typeoid;
	char	   *opname;
	bool		isArray;
	bool		useOr;
}	FdwBaseQual;

typedef struct FdwConstQual
{
	FdwBaseQual base;
	Datum		value;
	bool		isnull;
}	FdwConstQual;

typedef struct FdwVarQual
{
	FdwBaseQual base;
	AttrNumber	rightvarattno;
}	FdwVarQual;

typedef struct FdwParamQual
{
	FdwBaseQual base;
	Expr	   *expr;
}	FdwParamQual;

typedef struct FdwDeparsedSortGroup
{
	Name 			attname;
	int				attnum;
	bool			reversed;
	bool			nulls_first;
	Name			collate;
	PathKey	*key;
} FdwDeparsedSortGroup;

// datum.c
char *datumString(Datum datum, ConversionInfo *cinfo);
int64   datumInt64(Datum datum, ConversionInfo *cinfo);
inet *datumInet(Datum datum, ConversionInfo *cinfo);
inet *datumCIDR(Datum datum, ConversionInfo *cinfo);
double  datumDouble(Datum datum, ConversionInfo *cinfo);
bool  datumBool(Datum datum, ConversionInfo *cinfo);
Timestamp datumDate(Datum datum, ConversionInfo *cinfo);
Timestamp datumTimestamp(Datum datum, ConversionInfo *cinfo);

// query.c
void   extractRestrictions(Relids base_relids, Expr *node, List **quals);
void displayRestriction(PlannerInfo *root, Relids base_relids,RestrictInfo * r);
List  *extractColumns(List *reltargetlist, List *restrictinfolist);
void   initConversioninfo(ConversionInfo ** cinfo, AttInMetadata *attinmeta);
Value *colnameFromVar(Var *var, PlannerInfo *root, FdwPlanState * state);
void   computeDeparsedSortGroup(List *deparsed, FdwPlanState *planstate, List **apply_pathkeys, List **deparsed_pathkeys);
List  *findPaths(PlannerInfo *root, RelOptInfo *baserel, List *possiblePaths, int startupCost, FdwPlanState *state, List *apply_pathkeys, List *deparsed_pathkeys);
List  *deparse_sortgroup(PlannerInfo *root, Oid foreigntableid, RelOptInfo *rel);
List  *serializeDeparsedSortGroup(List *pathkeys);
List  *deserializeDeparsedSortGroup(List *items);


#endif // FDW_COMMON_H