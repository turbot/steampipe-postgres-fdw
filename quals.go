package main

/*
#cgo CFLAGS: -Ifdw -Ifdw/include/postgresql/server -Ifdw/include/postgresql/internal
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "fdw_helpers.h"
*/
import "C"

import (
	"fmt"
	"log"
	"net"
	"unsafe"

	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

func RestrictionsToQuals(node *C.ForeignScanState, cinfos **C.ConversionInfo) []*proto.Qual {
	plan := (*C.ForeignScan)(unsafe.Pointer(node.ss.ps.plan))
	restrictions := plan.fdw_exprs

	var qualsList qualList
	if restrictions == nil {
		return qualsList.quals
	}

	for it := restrictions.head; it != nil; it = it.next {
		restriction := C.cellGetExpr(it)
		log.Printf("[INFO] restriction %s     ************", C.GoString(C.tagTypeToString(C.fdw_nodeTag(restriction))))

		switch C.fdw_nodeTag(restriction) {
		case C.T_OpExpr:
			if q := qualFromOpExpr(C.cellGetOpExpr(it), node, cinfos); q != nil {
				qualsList.append(q)
			}
			break
		case C.T_ScalarArrayOpExpr:
			if q := qualFromScalarOpExpr(C.cellGetScalarArrayOpExpr(it), node, cinfos); q != nil {
				qualsList.append(q)
			}
			break
		case C.T_NullTest:
			//extractClauseFromNullTest(base_relids,				(NullTest *) node, quals);
			break
		case C.T_BooleanTest:
			//extractClauseFromBooleanTest(base_relids,				(BooleanTest *) node,			quals);
			break
		case C.T_BoolExpr:
			//extractClauseFromBooleanTest(base_relids,				(BooleanTest *) node,			quals);
			break
		}

	}
	log.Printf("[INFO] RestrictionsToQuals: converted postgres restrictions protobuf quals")
	//for _, q := range qualsList.quals {
	//	log.Printf("[INFO] %s", grpc.QualToString(q))
	//}
	return qualsList.quals
}

// TODO UPDATE COMMENT
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

func qualFromOpExpr(restriction *C.OpExpr, node *C.ForeignScanState, cinfos **C.ConversionInfo) *proto.Qual {
	plan := (*C.ForeignScan)(unsafe.Pointer(node.ss.ps.plan))
	relids := C.bms_make_singleton(C.int(plan.scan.scanrelid))

	restriction = C.canonicalOpExpr(restriction, relids)

	if restriction == nil {
		log.Printf("[WARN] could not convert OpExpr to canonical form - NOT adding qual for OpExpr")
		return nil
	}

	left := (*C.Var)(C.list_nth(restriction.args, 0))
	right := C.list_nth(restriction.args, 1)

	// Do not add it if it either contains a mutable function, or makes self references in the right hand side.
	if C.contain_volatile_functions((*C.Node)(right)) || C.bms_is_subset(relids, C.pull_varnos((*C.Node)(right))) {
		log.Printf("[WARN] restriction either contains a mutable function, or makes self references in the right hand side - NOT adding qual for OpExpr")
		return nil
	}

	arrayIndex := left.varattno - 1
	ci := C.getConversionInfo(cinfos, C.int(arrayIndex))
	qualValue, err := getQualValue(right, node, ci)
	if err != nil {
		log.Printf("[INFO] failed to convert qual value; %v", err)
		return nil
	}

	column := C.GoString(ci.attrname)
	operatorName := C.GoString(C.getOperatorString(restriction.opno))
	qual := &proto.Qual{
		FieldName: column,
		Operator:  &proto.Qual_StringValue{StringValue: operatorName},
		Value:     qualValue,
	}

	return qual
}

func qualFromScalarOpExpr(restriction *C.ScalarArrayOpExpr, node *C.ForeignScanState, cinfos **C.ConversionInfo) *proto.Qual {
	plan := (*C.ForeignScan)(unsafe.Pointer(node.ss.ps.plan))
	relids := C.bms_make_singleton(C.int(plan.scan.scanrelid))

	log.Printf("[INFO] qualFromOpExpr rel %+v is member %v, %s", relids, C.bms_is_member(1, relids), C.GoString(C.nodeToString(unsafe.Pointer(restriction))))
	restriction = C.canonicalScalarArrayOpExpr(restriction, relids)

	if restriction == nil {
		log.Printf("[WARN] could not convert OpExpr to canonical form - NOT adding qual for OpExpr")
		return nil
	}

	left := (*C.Var)(C.list_nth(restriction.args, 0))
	right := C.list_nth(restriction.args, 1)

	// Do not add it if it either contains a mutable function, or makes self references in the right hand side.
	if C.contain_volatile_functions((*C.Node)(right)) || C.bms_is_subset(relids, C.pull_varnos((*C.Node)(right))) {
		log.Printf("[WARN] restriction either contains a mutable function, or makes self references in the right hand side - NOT adding qual for OpExpr")
		return nil
	}

	arrayIndex := left.varattno - 1
	ci := C.getConversionInfo(cinfos, C.int(arrayIndex))
	qualValue, err := getQualValue(right, node, ci)
	if err != nil {
		log.Printf("[INFO] failed to convert qual value; %v", err)
		return nil
	}

	column := C.GoString(ci.attrname)
	operatorName := C.GoString(C.getOperatorString(restriction.opno))
	qual := &proto.Qual{
		FieldName: column,
		Operator:  &proto.Qual_StringValue{StringValue: operatorName},
		Value:     qualValue,
	}

	return qual
}

func getQualValue(right unsafe.Pointer, node *C.ForeignScanState, ci *C.ConversionInfo) (*proto.QualValue, error) {
	var isNull C.bool
	var typeOid C.Oid
	var value C.Datum
	valueExpression := (*C.Expr)(right)
	switch C.fdw_nodeTag(valueExpression) {
	case C.T_Const:
		constQual := (*C.Const)(right)
		typeOid = constQual.consttype
		value = constQual.constvalue
		isNull = constQual.constisnull
		break
	case C.T_Param:
		paramQual := (*C.Param)(right)
		typeOid = paramQual.paramtype
		exprState := C.ExecInitExpr(valueExpression, (*C.PlanState)(unsafe.Pointer(node)))
		econtext := node.ss.ps.ps_ExprContext
		value = C.ExecEvalExpr(exprState, econtext, &isNull)
		break
	default:
		return nil, fmt.Errorf("QualDefsToQuals: non-const qual value (type %v), skipping\n", C.fdw_nodeTag(valueExpression))
	}

	var qualValue *proto.QualValue
	if isNull {
		log.Printf("[DEBUG] qualDef.isnull=true - returning qual with nil value")
		qualValue = nil
	} else {
		if typeOid == C.InvalidOid {
			typeOid = ci.atttypoid
		}
		var err error
		if qualValue, err = datumToQualValue(value, typeOid, ci); err != nil {
			return nil, err
		}
	}
	return qualValue, nil
}

func datumToQualValue(datum C.Datum, typeOid C.Oid, cinfo *C.ConversionInfo) (*proto.QualValue, error) {
	/* we support these postgres column types (see sqlTypeForColumnType):
	 bool
	 bigint
	 double precision
	 text
	 inet
	 cidr
	 jsonb
	 timestamp

	so we must handle quals of all these type

	*/
	log.Printf("[WARN] datumToQualValue: convert postgres datum to protobuf qual value datum: %v, typeOid: %v\n", datum, typeOid)
	var result = &proto.QualValue{}

	switch typeOid {
	case C.TEXTOID, C.VARCHAROID:
		result.Value = &proto.QualValue_StringValue{StringValue: C.GoString(C.datumString(datum, cinfo))}
	case C.INETOID:

		inet := C.datumInet(datum, cinfo)
		ipAddrBytes := C.GoBytes(unsafe.Pointer(C.ipAddr(inet)), 16)
		netmaskBits := int32(C.netmaskBits(inet))
		var ipAddrString string
		var protocolVersion string
		if C.isIpV6(inet) {
			ipAddrString = net.IP(ipAddrBytes).String()
			protocolVersion = grpc.IPv6
			log.Printf("[TRACE] ipv6 qual: %s/%d", ipAddrString, netmaskBits)
		} else {
			ipAddrString = net.IPv4(ipAddrBytes[0], ipAddrBytes[1], ipAddrBytes[2], ipAddrBytes[3]).String()
			protocolVersion = grpc.IPv4
			log.Printf("[TRACE] ipv4 qual: %s/%d", ipAddrString, netmaskBits)
		}
		result.Value = &proto.QualValue_InetValue{
			InetValue: &proto.Inet{
				Mask:            netmaskBits,
				Addr:            ipAddrString,
				Cidr:            fmt.Sprintf("%s/%d", ipAddrString, netmaskBits),
				ProtocolVersion: protocolVersion,
			},
		}
	case C.DATEOID:
		pgts := int64(C.datumDate(datum, cinfo))
		result.Value = &proto.QualValue_TimestampValue{TimestampValue: PgTimeToTimestamp(pgts)}
	case C.TIMESTAMPOID:
		pgts := int64(C.datumTimestamp(datum, cinfo))
		result.Value = &proto.QualValue_TimestampValue{TimestampValue: PgTimeToTimestamp(pgts)}
	case C.INT2OID, C.INT4OID, C.INT8OID:
		result.Value = &proto.QualValue_Int64Value{Int64Value: int64(C.datumInt64(datum, cinfo))}
	case C.FLOAT4OID:
		result.Value = &proto.QualValue_DoubleValue{DoubleValue: float64(C.datumDouble(datum, cinfo))}
	case C.BOOLOID:
		result.Value = &proto.QualValue_BoolValue{BoolValue: bool(C.datumBool(datum, cinfo))}
	default:

		tuple := C.fdw_searchSysCache1(C.TYPEOID, C.fdw_objectIdGetDatum(typeOid))
		if !C.fdw_heapTupleIsValid(tuple) {
			return nil, fmt.Errorf("lookup failed for type %v", typeOid)
		}
		typeStruct := (C.Form_pg_type)(unsafe.Pointer(C.fdw_getStruct(tuple)))
		C.ReleaseSysCache(tuple)

		if (typeStruct.typelem != 0) && (typeStruct.typlen == -1) {
			log.Printf("[TRACE] datum is an array")
			return datumArrayToQualValue(datum, typeOid, cinfo)
		}
		log.Printf("[ERROR] unknown qual value: %s")

		return nil, fmt.Errorf("Unknown qual type %v", typeOid)
	}

	return result, nil

}

func datumArrayToQualValue(datum C.Datum, typeOid C.Oid, cinfo *C.ConversionInfo) (*proto.QualValue, error) {
	iterator := C.array_create_iterator(C.fdw_datumGetArrayTypeP(datum), 0, nil)

	var qualValues []*proto.QualValue
	var elem C.Datum
	var isNull C.bool
	for C.array_iterate(iterator, &elem, &isNull) {
		if isNull == C.bool(true) {
			log.Printf("[TRACE] datumArrayToQualValue: null qual value: %v", isNull)
			log.Println(isNull)
			qualValues = append(qualValues, nil)
			continue
		}

		tuple := C.fdw_searchSysCache1(C.TYPEOID, C.fdw_objectIdGetDatum(typeOid))
		if !C.fdw_heapTupleIsValid(tuple) {
			return nil, fmt.Errorf("lookup failed for type %v", typeOid)
		}
		typeStruct := (C.Form_pg_type)(unsafe.Pointer(C.fdw_getStruct(tuple)))
		C.ReleaseSysCache(tuple)
		if qualValue, err := datumToQualValue(elem, typeStruct.typelem, cinfo); err != nil {
			return nil, err
		} else {
			log.Printf("[TRACE datumArrayToQualValue: successfully converted qual - adding qual value %v", qualValue)
			qualValues = append(qualValues, qualValue)
		}
	}
	var result = &proto.QualValue{
		Value: &proto.QualValue_ListValue{
			ListValue: &proto.QualValueList{
				Values: qualValues,
			},
		},
	}
	log.Printf("[TRACE] datumArrayToQualValue complete, returning array of %d quals values \n", len(qualValues))

	return result, nil
}

//
//
//void extractClauseFromBooleanTest(Relids base_relids,
//BooleanTest *node,
//List **quals){
//// IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE, IS_UNKNOWN, IS_NOT_UNKNOWN
//elog(INFO, "extractClauseFromBooleanTest, xpr %s, arg %s, booltesttype %u, location %d", nodeToString(&(node->xpr)),  nodeToString(node->arg), node->booltesttype, node->location);
//
//}

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

/*	Convert a "NullTest" (IS NULL, or IS NOT NULL)
 *	to a suitable intermediate representation.
 */
//void
//extractClauseFromNullTest(Relids base_relids,
//NullTest *node,
//List **quals)
//{
//if (IsA(node->arg, Var))
//{
//Var		   *var = (Var *) node->arg;
//FdwBaseQual *result;
//char	   *opname = NULL;
//
//if (var->varattno < 1)
//{
//return;
//}
//if (node->nulltesttype == IS_NULL)
//{
//opname = "=";
//}
//else
//{
//opname = "<>";
//}
//result = makeQual(var->varattno, opname,
//(Expr *) makeNullConst(INT4OID, -1, InvalidOid),
//false,
//false);
//*quals = lappend(*quals, result);
//}
//}

/* qualsList is a wrapper for a listy of grpc quals performs duplicate checking before adding to the list
   this was needed as we found for some qeuries we get duplicate quals, for example:

 select
  u.name as username,
  s.decision,
  jsonb_pretty(s.matched_statements)
from
  morales.aws_iam_user as u,
  morales.aws_iam_policy_simulator as s
where
  s.action = 's3:DeleteBucket'
  and s.resource_arn = '*'
  and s.principal_arn = u.arn;


field 'action' operator '&{=}' value 'string_value:"s3:DeleteBucket"'
field 'resource_arn' operator '&{=}' value 'string_value:"*"'
field 'principal_arn' operator '&{=}' value 'string_value:"?"'
field 'action' operator '&{=}' value 'string_value:"s3:DeleteBucket"'
field 'resource_arn' operator '&{=}' value 'string_value:"*"'
fieldName:"action"  string_value:"="

*/
type qualList struct {
	quals []*proto.Qual
}

// append the qual to our list, checking for duplicates
func (q *qualList) append(qual *proto.Qual) {
	if !q.contains(qual) {
		q.quals = append(q.quals, qual)
	}
}

func (q *qualList) contains(other *proto.Qual) bool {
	for _, qual := range q.quals {
		if grpc.QualEquals(qual, other) {
			return true
		}
	}
	return false
}
