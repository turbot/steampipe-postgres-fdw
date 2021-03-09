#include "steampipe_postgres_fdw.h"
#include "common.h"
#if PG_VERSION_NUM < 120000
#include "optimizer/var.h"
#else
#include "optimizer/optimizer.h"
#endif
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/subselect.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_operator.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "nodes/value.h"
#include "pg_config.h"

/* Third argument to get_attname was introduced in [8237f27] (release 11) */
#if PG_VERSION_NUM >= 110000
#define get_attname(x, y) get_attname(x, y, true)
#endif

void extractClauseFromOpExpr(Relids base_relids,
						OpExpr *node,
						List **quals);

void extractClauseFromNullTest(Relids base_relids,
						  NullTest *node,
						  List **quals);

void extractClauseFromScalarArrayOpExpr(Relids base_relids,
								   ScalarArrayOpExpr *node,
								   List **quals);

void extractClauseFromBooleanTest(Relids base_relids,
								   BooleanTest *node,
								   List **quals);

//void extractClauseFromBoolExpr(Relids base_relids,
//								   BoolExpr *node,
//								   List **quals);

char	   *getOperatorString(Oid opoid);

FdwBaseQual *makeQual(AttrNumber varattno, char *opname, Expr *value,
		 bool isarray,
		 bool useOr);


Node	   *unnestClause(Node *node);
void swapOperandsAsNeeded(Node **left, Node **right, Oid *opoid,
					 Relids base_relids);
OpExpr	   *canonicalOpExpr(OpExpr *opExpr, Relids base_relids);
ScalarArrayOpExpr *canonicalScalarArrayOpExpr(ScalarArrayOpExpr *opExpr,
						   Relids base_relids);

bool isAttrInRestrictInfo(Index relid, AttrNumber attno,
					 RestrictInfo *restrictinfo);

List *clausesInvolvingAttr(Index relid, AttrNumber attnum,
					 EquivalenceClass *eq_class);

Expr *fdw_get_em_expr(EquivalenceClass *ec, RelOptInfo *rel);

//void displayRestriction(Relids base_relids,RestrictInfo * r);


/*
 * The list of needed columns (represented by their respective vars)
 * is pulled from:
 *	- the targetcolumns
 *	- the restrictinfo
 */
List *
extractColumns(List *reltargetlist, List *restrictinfolist)
{
	ListCell   *lc;
	List	   *columns = NULL;
	int			i = 0;

	foreach(lc, reltargetlist)
	{
		List	   *targetcolumns;
		Node	   *node = (Node *) lfirst(lc);

		targetcolumns = pull_var_clause(node,
#if PG_VERSION_NUM >= 90600
										PVC_RECURSE_AGGREGATES|
										PVC_RECURSE_PLACEHOLDERS);
#else
										PVC_RECURSE_AGGREGATES,
										PVC_RECURSE_PLACEHOLDERS);
#endif
		columns = list_union(columns, targetcolumns);
		i++;
	}
	foreach(lc, restrictinfolist)
	{
		List	   *targetcolumns;
		RestrictInfo *node = (RestrictInfo *) lfirst(lc);

		targetcolumns = pull_var_clause((Node *) node->clause,
#if PG_VERSION_NUM >= 90600
										PVC_RECURSE_AGGREGATES|
										PVC_RECURSE_PLACEHOLDERS);
#else
										PVC_RECURSE_AGGREGATES,
										PVC_RECURSE_PLACEHOLDERS);
#endif
		columns = list_union(columns, targetcolumns);
	}
	return columns;
}

/*
 * Initialize the array of "ConversionInfo" elements, needed to convert go
 * objects back to suitable postgresql data structures.
 */
void
initConversioninfo(ConversionInfo ** cinfos, AttInMetadata *attinmeta)
{
	int			i;

	for (i = 0; i < attinmeta->tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(attinmeta->tupdesc,i);
		Oid			outfuncoid;
		bool		typIsVarlena;



		if (!attr->attisdropped)
		{
			ConversionInfo *cinfo = palloc0(sizeof(ConversionInfo));

			cinfo->attoutfunc = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
			getTypeOutputInfo(attr->atttypid, &outfuncoid, &typIsVarlena);
			fmgr_info(outfuncoid, cinfo->attoutfunc);
			cinfo->atttypoid = attr->atttypid;
			cinfo->atttypmod = attinmeta->atttypmods[i];
			cinfo->attioparam = attinmeta->attioparams[i];
			cinfo->attinfunc = &attinmeta->attinfuncs[i];
			cinfo->attrname = NameStr(attr->attname);
			cinfo->attnum = i + 1;
			cinfo->attndims = attr->attndims;
			cinfo->need_quote = false;
			cinfos[i] = cinfo;
		}
		else
		{
			cinfos[i] = NULL;
		}
	}
}


char *
getOperatorString(Oid opoid)
{
	HeapTuple	tp;
	Form_pg_operator operator;

	tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for operator %u", opoid);
	operator = (Form_pg_operator) GETSTRUCT(tp);
	ReleaseSysCache(tp);
	return NameStr(operator->oprname);
}


/*
 * Returns the node of interest from a node.
 */
Node *
unnestClause(Node *node)
{
	switch (node->type)
	{
		case T_RelabelType:
			return (Node *) ((RelabelType *) node)->arg;
		case T_ArrayCoerceExpr:
			return (Node *) ((ArrayCoerceExpr *) node)->arg;
		default:
			return node;
	}
}


void
swapOperandsAsNeeded(Node **left, Node **right, Oid *opoid,
					 Relids base_relids)
{
	HeapTuple	tp;
	Form_pg_operator op;
	Node	   *l = *left,
			   *r = *right;

	tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(*opoid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for operator %u", *opoid);
	op = (Form_pg_operator) GETSTRUCT(tp);
	ReleaseSysCache(tp);
	/* Right is already a var. */
	/* If "left" is a Var from another rel, and right is a Var from the */
	/* target rel, swap them. */
	/* Same thing is left is not a var at all. */
	/* To swap them, we have to lookup the commutator operator. */
	if (IsA(r, Var))
	{
		Var		   *rvar = (Var *) r;

		if (!IsA(l, Var) ||
			(!bms_is_member(((Var *) l)->varno, base_relids) &&
			 bms_is_member(rvar->varno, base_relids)))
		{
			/* If the operator has no commutator operator, */
			/* bail out. */
			if (op->oprcom == 0)
			{
				return;
			}
			{
				*left = r;
				*right = l;
				*opoid = op->oprcom;
			}
		}
	}

}

/*
 * Swaps the operands if needed / possible, so that left is always a node
 * belonging to the baserel and right is either:
 *	- a Const
 *	- a Param
 *	- a Var from another relation
 */
OpExpr *
canonicalOpExpr(OpExpr *opExpr, Relids base_relids)
{
	Oid			operatorid = opExpr->opno;
	Node	   *l,
			   *r;
	OpExpr	   *result = NULL;

    int length = (int)list_length(opExpr->args);
    elog(WARNING, "canonicalOpExpr, arg length: %d, base_relids %x", length, (int)base_relids);

	/* Only treat binary operators for now. */
	if (length == 2)
	{
		l = unnestClause(list_nth(opExpr->args, 0));
		r = unnestClause(list_nth(opExpr->args, 1));

		elog(WARNING, "l arg: %s", nodeToString(l));
		elog(WARNING, "r arg: %s", nodeToString(r));

		swapOperandsAsNeeded(&l, &r, &operatorid, base_relids);

		/* varno:	    index of this var's relation in the range table, or INNER_VAR/OUTER_VAR/INDEX_VAR
		   varattno:	attribute number of this var, or zero for all attrs ("whole-row Var") */

		if (IsA(l, Var)
		    && bms_is_member(((Var *) l)->varno, base_relids)
		 	&& ((Var *) l)->varattno >= 1)
		{
			result = (OpExpr *) make_opclause(operatorid,
											  opExpr->opresulttype,
											  opExpr->opretset,
											  (Expr *) l, (Expr *) r,
											  opExpr->opcollid,
											  opExpr->inputcollid);

          elog(WARNING, "canonicalOpExpr returning result");
		}
	} else {
	  elog(WARNING, "canonicalOpExpr - arg length %d, ignoring", length);
    }

	return result;
}

/*
 * Swaps the operands if needed / possible, so that left is always a node
 * belonging to the baserel and right is either:
 *	- a Const
 *	- a Param
 *	- a Var from another relation
 */
ScalarArrayOpExpr *
canonicalScalarArrayOpExpr(ScalarArrayOpExpr *opExpr,
						   Relids base_relids)
{
	Oid			operatorid = opExpr->opno;
	Node	   *l,
			   *r;
	ScalarArrayOpExpr *result = NULL;
	HeapTuple	tp;
	Form_pg_operator op;

	/* Only treat binary operators for now. */
	if (list_length(opExpr->args) == 2)
	{
		l = unnestClause(list_nth(opExpr->args, 0));
		r = unnestClause(list_nth(opExpr->args, 1));
		tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(operatorid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for operator %u", operatorid);
		op = (Form_pg_operator) GETSTRUCT(tp);
		ReleaseSysCache(tp);
		if (IsA(l, Var) &&bms_is_member(((Var *) l)->varno, base_relids)
			&& ((Var *) l)->varattno >= 1)
		{
			result = makeNode(ScalarArrayOpExpr);
			result->opno = operatorid;
			result->opfuncid = op->oprcode;
			result->useOr = opExpr->useOr;
			result->args = lappend(result->args, l);
			result->args = lappend(result->args, r);
			result->location = opExpr->location;

		}
	}
	return result;
}


/*
 * Extract conditions that can be pushed down, as well as the parameters.
 *
 */
void
extractRestrictions(Relids base_relids,
					Expr *node,
					List **quals)
{
    // see laurenz/oracle_fdw/oracle_fdw.c #4548

    elog(WARNING, "extractRestrictions, restriction type: %s",  tagTypeToString(nodeTag(node)));
	switch (nodeTag(node))
	{
		case T_OpExpr:
			extractClauseFromOpExpr(base_relids,
									(OpExpr *) node, quals);
			break;
		case T_NullTest:
			extractClauseFromNullTest(base_relids,
									  (NullTest *) node, quals);
			break;
		case T_ScalarArrayOpExpr:
			extractClauseFromScalarArrayOpExpr(base_relids,
											   (ScalarArrayOpExpr *) node,
											   quals);
			break;
        case T_BooleanTest:
			extractClauseFromBooleanTest(base_relids,
											   (BooleanTest *) node,
											   quals);
			break;
        case T_BoolExpr:
            elog(INFO, "T_BooleanExpr %d", ((BoolExpr *)node)->boolop);
            ListCell *cell;
            List* args = ((BoolExpr *)node)->args;
            // if there is a single variable argument, extract a bool qual
            if (list_length(args) == 1){
                elog(INFO, "bool expression with single arg");
            //
            }
	        foreach(cell, args)
            {
                Expr *e = (Expr *)lfirst(cell);
                elog(INFO, "arg: %s", nodeToString(e));
//                extractRestrictions(base_relids, (Expr *)lfirst(cell), quals);
                // Store only a Value node containing the string name of the column.
                // if (nodeTag(e) == T_Var){
                //     Value* v = colnameFromVar((Var*)e, root, NULL);
                //     char* colname = (((Value *)(v))->val.str);
                //     if (colname != NULL && strVal(colname) != NULL) {
                //     elog(INFO, "col: %s", colname);

                // }
                //}
            }
            elog(INFO, "END T_BooleanExpr");
            break;
        case T_Var:
            elog(INFO, "T_Var: %s", nodeToString(node));
            break;
		default:

//				ereport(WARNING,
//						(errmsg("unsupported expression for "
//								"extractClauseFrom"),
//						 errdetail("%s", nodeToString(node))));

            elog(WARNING, "unsupported expression for extractClauseFrom: %s", nodeToString(node));

			break;
	}
	elog(INFO, "RETURN");
}

void displayRestriction(PlannerInfo *root, Relids base_relids, RestrictInfo * r){
    elog(INFO, "displayRestrictions");
    Expr *node = r->clause;
    elog(INFO, "restriction type: %s",  tagTypeToString(nodeTag(node)));
    elog(INFO, "node: %s", nodeToString(node));

//    __builtin_dump_struct(r, &dumpStruct.clause);

}

/*
 *	Build an intermediate value representation for an OpExpr,
 *	and append it to the corresponding list (quals, or params).
 *
 *	The quals list consist of list of the form:
 *
 *	- Const key: the column index in the cinfo array
 *	- Const operator: the operator representation
 *	- Var or Const value: the value.
 */
void
extractClauseFromOpExpr(Relids base_relids,
						OpExpr *op,
						List **quals)
{
	Var		   *left;
	Expr	   *right;
    ListCell   *lc;
    foreach(lc, op->args){

    }
	/* Use a "canonical" version of the op expression, to ensure that the */
	/* left operand is a Var on our relation. */
	op = canonicalOpExpr(op, base_relids);
	if (op)
	{
	    elog(INFO, "got op from canonicalOpExpr");

		left = list_nth(op->args, 0);
		right = list_nth(op->args, 1);
		/* Do not add it if it either contains a mutable function, or makes */
		/* self references in the right hand side. */
		if (!(contain_volatile_functions((Node *) right) ||
			  bms_is_subset(base_relids, pull_varnos((Node *) right))))
		{
		    elog(INFO, "adding qual for OpExpr opno %d", op->opno);
			*quals = lappend(*quals, makeQual(left->varattno,
											  getOperatorString(op->opno),
											  right, false, false));
		} else {
		    elog(INFO, "NOT adding qual for OpExpr");
		}
	}
}

void
extractClauseFromScalarArrayOpExpr(Relids base_relids,
								   ScalarArrayOpExpr *op,
								   List **quals)
{
	Var		   *left;
	Expr	   *right;

	op = canonicalScalarArrayOpExpr(op, base_relids);
	if (op)
	{
		left = list_nth(op->args, 0);
		right = list_nth(op->args, 1);
		if (!(contain_volatile_functions((Node *) right) ||
			  bms_is_subset(base_relids, pull_varnos((Node *) right))))
		{
			*quals = lappend(*quals, makeQual(left->varattno,
											  getOperatorString(op->opno),
											  right, true,
											  op->useOr));
		}
	}
}

void extractClauseFromBooleanTest(Relids base_relids,
								   BooleanTest *node,
								   List **quals){
    // IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE, IS_UNKNOWN, IS_NOT_UNKNOWN
    elog(INFO, "extractClauseFromBooleanTest, xpr %s, arg %s, booltesttype %u, location %d", nodeToString(&(node->xpr)),  nodeToString(node->arg), node->booltesttype, node->location);

}
//
//void extractClauseFromBoolExpr(Relids base_relids,
//								   BoolExpr *node,
//								   List **quals){
//
//    FdwBoolExprQual* qual = palloc0(sizeof(FdwBoolExprQual));
//    qual->typeoid = ((Const *) value)->T_BoolExpr;
//
//    qual->args = node.args
//    quals->node.boolop
//
//      foreach(cell, args)
//                   {
//                       Expr *e = (Expr *)lfirst(cell);
//                       elog(INFO, "arg: %s", nodeToString(e));
//        //                extractRestrictions(base_relids, (Expr *)lfirst(cell), quals);
//                       // Store only a Value node containing the string name of the column.
//                       if (nodeTag(e) == T_Var){
//                           Value* v = colnameFromVar((Var*)e, root, NULL);
//                           char* colname = (((Value *)(v))->val.str);
//                           if (colname != NULL && strVal(colname) != NULL) {
//                           elog(INFO, "col: %s", colname);
//
//                       }
//                       }
//                   }
//
//    elog(INFO, "T_BooleanExpr %d", ((BoolExpr *)node)->boolop);
//               ListCell *cell;
//               List* args = ((BoolExpr *)node)->args;
//               // if there is a single variable argument, extract a bool qual
//               if (list_length(args) == 1){
//                   elog(INFO, "bool expression with single arg");
//               //
//               }
//
//               elog(INFO, "END T_BooleanExpr");
//    // IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE, IS_UNKNOWN, IS_NOT_UNKNOWN
//    elog(INFO, "extractClauseFromBooleanTest, xpr %s, arg %s, booltesttype %u, location %d", nodeToString(&(node->xpr)),  nodeToString(node->arg), node->booltesttype, node->location);
//
//}
//

/*
 *	Convert a "NullTest" (IS NULL, or IS NOT NULL)
 *	to a suitable intermediate representation.
 */
void
extractClauseFromNullTest(Relids base_relids,
						  NullTest *node,
						  List **quals)
{
	if (IsA(node->arg, Var))
	{
		Var		   *var = (Var *) node->arg;
		FdwBaseQual *result;
		char	   *opname = NULL;

		if (var->varattno < 1)
		{
			return;
		}
		if (node->nulltesttype == IS_NULL)
		{
			opname = "=";
		}
		else
		{
			opname = "<>";
		}
		result = makeQual(var->varattno, opname,
						  (Expr *) makeNullConst(INT4OID, -1, InvalidOid),
						  false,
						  false);
		*quals = lappend(*quals, result);
	}
}



/*
 *	Returns a "Value" node containing the string name of the column from a var.
 */
Value *
colnameFromVar(Var *var, PlannerInfo *root, FdwPlanState * planstate)
{
	RangeTblEntry *rte = rte = planner_rt_fetch(var->varno, root);

//    elog(INFO, "colnameFromVar relid %d, varattno %d", rte->relid, var->varattno);
	char	   *attname = get_attname(rte->relid, var->varattno);

	if (attname == NULL)
	{
		return NULL;
	}
	else
	{
		return makeString(attname);
	}
}

/*
 *	Build an opaque "qual" object.
 */
FdwBaseQual *
makeQual(AttrNumber varattno, char *opname, Expr *value, bool isarray, bool useOr)
{
	FdwBaseQual *qual;

    elog(INFO, "makeQual varattno: %d, opname: %s, value: %s, isarray: %d", varattno, opname, nodeToString(value), isarray);
    elog(INFO, "qual value type: %s",  tagTypeToString(nodeTag(value)));

	switch (value->type)
	{
		case T_Const:
		    qual = palloc0(sizeof(FdwConstQual));
			qual->right_type = T_Const;
			qual->typeoid = ((Const *) value)->consttype;
			((FdwConstQual *) qual)->value = ((Const *) value)->constvalue;
			((FdwConstQual *) qual)->isnull = ((Const *) value)->constisnull;
			break;
		case T_Var:
			elog(INFO, "T_Var");
            qual = palloc0(sizeof(FdwVarQual));
			qual->right_type = T_Var;
			((FdwVarQual *) qual)->rightvarattno = ((Var *) value)->varattno;
			break;
		default:
		    elog(INFO, "other value type %s", nodeToString(value));
			qual = palloc0(sizeof(FdwParamQual));
			qual->right_type = T_Param;
			((FdwParamQual *) qual)->expr = value;
			qual->typeoid = InvalidOid;
			break;
	}
	qual->varattno = varattno;
	qual->opname = opname;
	qual->isArray = isarray;
	qual->useOr = useOr;

	return qual;
}

/*
 *	Test whether an attribute identified by its relid and attno
 *	is present in a list of restrictinfo
 */
bool
isAttrInRestrictInfo(Index relid, AttrNumber attno, RestrictInfo *restrictinfo)
{
	List	   *vars = pull_var_clause((Node *) restrictinfo->clause,
#if PG_VERSION_NUM >= 90600
										PVC_RECURSE_AGGREGATES|
										PVC_RECURSE_PLACEHOLDERS);
#else
										PVC_RECURSE_AGGREGATES,
										PVC_RECURSE_PLACEHOLDERS);
#endif
	ListCell   *lc;

	foreach(lc, vars)
	{
		Var		   *var = (Var *) lfirst(lc);

		if (var->varno == relid && var->varattno == attno)
		{
			return true;
		}

	}
	return false;
}

List *
clausesInvolvingAttr(Index relid, AttrNumber attnum,
					 EquivalenceClass *ec)
{
	List	   *clauses = NULL;

	/*
	 * If there is only one member, then the equivalence class is either for
	 * an outer join, or a desired sort order. So we better leave it
	 * untouched.
	 */
	if (ec->ec_members->length > 1)
	{
		ListCell   *ri_lc;

		foreach(ri_lc, ec->ec_sources)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(ri_lc);

			if (isAttrInRestrictInfo(relid, attnum, ri))
			{
				clauses = lappend(clauses, ri);
			}
		}
	}
	return clauses;
}

/*
 * Given a list of FdwDeparsedSortGroup and a FdwPlanState,
 * construct a list of PathKey and FdwDeparsedSortGroup that belongs to
 * the FDW and that the FDW say it can enforce.
 */
void computeDeparsedSortGroup(List *deparsed, FdwPlanState *planstate,
		List **apply_pathkeys,
		List **deparsed_pathkeys)
{
	List		*sortable_fields = NULL;
	ListCell	*lc, *lc2;

	/* Both lists should be empty */
	Assert(*apply_pathkeys == NIL);
	Assert(*deparsed_pathkeys == NIL);

	/* Don't ask FDW if nothing to sort */
	if (deparsed == NIL)
		return;

  // TODO - Fdw doesn't support this yet
	//sortable_fields = canSort(planstate, deparsed);
  sortable_fields = NIL;

	/* Don't go further if FDW can't enforce any sort */
	if (sortable_fields == NIL)
		return;

	foreach(lc, sortable_fields)
	{
		FdwDeparsedSortGroup *sortable_md = (FdwDeparsedSortGroup *) lfirst(lc);
		foreach(lc2, deparsed)
		{
			FdwDeparsedSortGroup *wanted_md = lfirst(lc2);

			if (sortable_md->attnum == wanted_md->attnum)
			{
				*apply_pathkeys = lappend(*apply_pathkeys, wanted_md->key);
				*deparsed_pathkeys = lappend(*deparsed_pathkeys, wanted_md);
			}
		}
	}
}


List *
findPaths(PlannerInfo *root, RelOptInfo *baserel, List *possiblePaths,
		int startupCost,
		FdwPlanState *state,
		List *apply_pathkeys, List *deparsed_pathkeys)
{
	List	   *result = NULL;
	ListCell   *lc;

	foreach(lc, possiblePaths)
	{
		List	   *item = lfirst(lc);
		List	   *attrnos = linitial(item);
		ListCell   *attno_lc;
		int			nbrows = ((Const *) lsecond(item))->constvalue;
		List	   *allclauses = NULL;
		Bitmapset  *outer_relids = NULL;

		/* Armed with this knowledge, look for a join condition */
		/* matching the path list. */
		/* Every key must be present in either, a join clause or an */
		/* equivalence_class. */
		foreach(attno_lc, attrnos)
		{
			AttrNumber	attnum = lfirst_int(attno_lc);
			ListCell   *lc;
			List	   *clauses = NULL;

			/* Look in the equivalence classes. */
			foreach(lc, root->eq_classes)
			{
				EquivalenceClass *ec = (EquivalenceClass *) lfirst(lc);
				List	   *ec_clauses = clausesInvolvingAttr(baserel->relid,
															  attnum,
															  ec);

				clauses = list_concat(clauses, ec_clauses);
				if (ec_clauses != NIL)
				{
					outer_relids = bms_union(outer_relids, ec->ec_relids);
				}
			}
			/* Do the same thing for the outer joins */
			foreach(lc, list_union(root->left_join_clauses,
								   root->right_join_clauses))
			{
				RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

				if (isAttrInRestrictInfo(baserel->relid, attnum, ri))
				{
					clauses = lappend(clauses, ri);
					outer_relids = bms_union(outer_relids,
											 ri->outer_relids);

				}
			}
			/* We did NOT find anything for this key, bail out */
			if (clauses == NIL)
			{
				allclauses = NULL;
				break;
			}
			else
			{
				allclauses = list_concat(allclauses, clauses);
			}
		}
		/* Every key has a corresponding restriction, we can build */
		/* the parameterized path and add it to the plan. */
		if (allclauses != NIL)
		{
			Bitmapset  *req_outer = bms_difference(outer_relids,
										 bms_make_singleton(baserel->relid));
			ParamPathInfo *ppi;
			ForeignPath *foreignPath;

			if (!bms_is_empty(req_outer))
			{
				ppi = makeNode(ParamPathInfo);
				ppi->ppi_req_outer = req_outer;
				ppi->ppi_rows = nbrows;
				ppi->ppi_clauses = list_concat(ppi->ppi_clauses, allclauses);
				/* Add a simple parameterized path */
				foreignPath = create_foreignscan_path(
													  root, baserel,
#if PG_VERSION_NUM >= 90600
												 	  NULL,  /* default pathtarget */
#endif
													  nbrows,
													  startupCost,
#if PG_VERSION_NUM >= 90600
													  nbrows * baserel->reltarget->width,
#else
													  nbrows * baserel->width,
#endif
													  NIL, /* no pathkeys */
													  NULL,
#if PG_VERSION_NUM >= 90500
													  NULL,
#endif
													  NULL);

				foreignPath->path.param_info = ppi;
				result = lappend(result, foreignPath);
			}
		}
	}
	return result;
}

/*
 * Deparse a list of PathKey and return a list of FdwDeparsedSortGroup.
 * This function will return data iif all the PathKey belong to the current
 * foreign table.
 */
List *
deparse_sortgroup(PlannerInfo *root, Oid foreigntableid, RelOptInfo *rel)
{
	List *result = NULL;
	ListCell   *lc;

	/* return empty list if no pathkeys for the PlannerInfo */
	if (! root->query_pathkeys)
		return NIL;

	foreach(lc,root->query_pathkeys)
	{
		PathKey *key = (PathKey *) lfirst(lc);
		FdwDeparsedSortGroup *md = palloc0(sizeof(FdwDeparsedSortGroup));
		EquivalenceClass *ec = key->pk_eclass;
		Expr *expr;
		bool found = false;

		if ((expr = fdw_get_em_expr(ec, rel)))
		{
			md->reversed = (key->pk_strategy == BTGreaterStrategyNumber);
			md->nulls_first = key->pk_nulls_first;
			md->key = key;

			if (IsA(expr, Var))
			{
				Var *var = (Var *) expr;
				md->attname = (Name) strdup(get_attname(foreigntableid, var->varattno));
				md->attnum = var->varattno;
				found = true;
			}
			/* ORDER BY clauses having a COLLATE option will be RelabelType */
			else if (IsA(expr, RelabelType) &&
					IsA(((RelabelType *) expr)->arg, Var))
			{
				Var *var = (Var *)((RelabelType *) expr)->arg;
				Oid collid = ((RelabelType *) expr)->resultcollid;

				if (collid == DEFAULT_COLLATION_OID)
					md->collate = NULL;
				else
					md->collate = (Name) strdup(get_collation_name(collid));
				md->attname = (Name) strdup(get_attname(foreigntableid, var->varattno));
				md->attnum = var->varattno;
				found = true;
			}
		}

		if (found)
			result = lappend(result, md);
		else
		{
			/* pfree() current entry */
			pfree(md);
			/* pfree() all previous entries */
			while ((lc = list_head(result)) != NULL)
			{
				md = (FdwDeparsedSortGroup *) lfirst(lc);
				result = list_delete_ptr(result, md);
				pfree(md);
			}
			break;
		}
	}

	return result;
}

Expr *
fdw_get_em_expr(EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell   *lc_em;

	foreach(lc_em, ec->ec_members)
	{
		EquivalenceMember *em = lfirst(lc_em);

		if (bms_equal(em->em_relids, rel->relids))
		{
			/*
			 * If there is more than one equivalence member whose Vars are
			 * taken entirely from this relation, we'll be content to choose
			 * any one of those.
			 */
			return em->em_expr;
		}
	}

	/* We didn't find any suitable equivalence class expression */
	return NULL;
}

List *
serializeDeparsedSortGroup(List *pathkeys)
{
	List *result = NIL;
	ListCell *lc;

	foreach(lc, pathkeys)
	{
		List *item = NIL;
		FdwDeparsedSortGroup *key = (FdwDeparsedSortGroup *)
			lfirst(lc);

		item = lappend(item, makeString(NameStr(*(key->attname))));
		item = lappend(item, makeInteger(key->attnum));
		item = lappend(item, makeInteger(key->reversed));
		item = lappend(item, makeInteger(key->nulls_first));
		if(key->collate != NULL)
			item = lappend(item, makeString(NameStr(*(key->collate))));
		else
			item = lappend(item, NULL);
		item = lappend(item, key->key);

		result = lappend(result, item);
	}

	return result;
}

List *
deserializeDeparsedSortGroup(List *items)
{
	List *result = NIL;
	ListCell *k;

	foreach(k, items)
	{
		ListCell *lc;
		FdwDeparsedSortGroup *key =
			palloc0(sizeof(FdwDeparsedSortGroup));

		lc = list_head(lfirst(k));
		key->attname = (Name) strdup(strVal(lfirst(lc)));

		lc = lnext(lc);
		key->attnum = (int) intVal(lfirst(lc));

		lc = lnext(lc);
		key->reversed = (bool) intVal(lfirst(lc));

		lc = lnext(lc);
		key->nulls_first = (bool) intVal(lfirst(lc));

		lc = lnext(lc);
		if(lfirst(lc) != NULL)
			key->collate = (Name) strdup(strVal(lfirst(lc)));
		else
			key->collate = NULL;

		lc = lnext(lc);
		key->key = (PathKey *) lfirst(lc);

		result = lappend(result, key);
	}

	return result;
}




