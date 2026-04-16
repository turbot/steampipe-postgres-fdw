
#include "common.h"
#include "fdw_helpers.h"

// convert NodeTag to string. At the moment it only handles primitive types, bu twoul dbe easy to add all if neeeded
char* tagTypeToString(NodeTag type)
{
    char *tagNames[] ={
    "T_Alias",
    "T_RangeVar",
    "T_TableFunc",
    "T_Expr",
    "T_Var",
    "T_Const",
    "T_Param",
    "T_Aggref",
    "T_GroupingFunc",
    "T_WindowFunc",
    "T_SubscriptingRef",
    "T_FuncExpr",
    "T_NamedArgExpr",
    "T_OpExpr",
    "T_DistinctExpr",
    "T_NullIfExpr",
    "T_ScalarArrayOpExpr",
    "T_BoolExpr",
    "T_SubLink",
    "T_SubPlan",
    "T_AlternativeSubPlan",
    "T_FieldSelect",
    "T_FieldStore",
    "T_RelabelType",
    "T_CoerceViaIO",
    "T_ArrayCoerceExpr",
    "T_ConvertRowtypeExpr",
    "T_CollateExpr",
    "T_CaseExpr",
    "T_CaseWhen",
    "T_CaseTestExpr",
    "T_ArrayExpr",
    "T_RowExpr",
    "T_RowCompareExpr",
    "T_CoalesceExpr",
    "T_MinMaxExpr",
    "T_SQLValueFunction",
    "T_XmlExpr",
    "T_NullTest",
    "T_BooleanTest",
    "T_CoerceToDomain",
    "T_CoerceToDomainValue",
    "T_SetToDefault",
    "T_CurrentOfExpr",
    "T_NextValueExpr",
    "T_InferenceElem",
    "T_TargetEntry",
    "T_RangeTblRef",
    "T_JoinExpr",
    "T_FromExpr",
    "T_OnConflictExpr",
    "T_IntoClause",
    "T_RestrictInfo"  /* Add RestrictInfo for PostgreSQL v16+ compatibility */
};
    int idx = (int)type - (int)T_Alias;
    if (idx <  sizeof(tagNames) / sizeof(tagNames[0])){
        return tagNames[idx];
    }

    /* DEBUG: Log unknown node types for PostgreSQL v16+ compatibility debugging */
    static char unknown_type_buf[64];
    snprintf(unknown_type_buf, sizeof(unknown_type_buf), "T_Unknown_%d", (int)type);
    return unknown_type_buf;

}
