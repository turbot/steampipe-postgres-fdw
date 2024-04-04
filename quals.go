package main

/*
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

	"github.com/gertd/go-pluralize"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/quals"
)

func singleRestrictionToQual(it *C.ListCell, node *C.ForeignScanState, cinfos *conversionInfos) *proto.Qual {
	restriction := C.cellGetExpr(it)
	log.Printf("[TRACE] SingleRestrictionToQual: restriction %s", C.GoString(C.tagTypeToString(C.fdw_nodeTag(restriction))))

	var q *proto.Qual
	switch C.fdw_nodeTag(restriction) {
	case C.T_OpExpr:
		q = qualFromOpExpr(C.cellGetOpExpr(it), node, cinfos)
	case C.T_Var:
		q = qualFromVar(C.cellGetVar(it), node, cinfos)

	case C.T_ScalarArrayOpExpr:
		q = qualFromScalarOpExpr(C.cellGetScalarArrayOpExpr(it), node, cinfos)
	case C.T_NullTest:
		q = qualFromNullTest(C.cellGetNullTest(it), node, cinfos)
		//extractClauseFromNullTest(base_relids,				(NullTest *) node, qualsList);
	case C.T_BooleanTest:
		q = qualFromBooleanTest((*C.BooleanTest)(unsafe.Pointer(restriction)), node, cinfos)
	case C.T_BoolExpr:
		q = qualFromBoolExpr((*C.BoolExpr)(unsafe.Pointer(restriction)), node, cinfos)
	}

	return q
}

func restrictionsToQuals(node *C.ForeignScanState, cinfos *conversionInfos) (qualsList *proto.Quals, unhandledRestrictions int) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] restrictionsToQuals recovered from panic: %v", r)
		}
	}()

	plan := (*C.ForeignScan)(unsafe.Pointer(node.ss.ps.plan))
	restrictions := plan.fdw_exprs

	qualsList = &proto.Quals{}
	if restrictions == nil {
		return qualsList, 0
	}

	for it := C.list_head(restrictions); it != nil; it = C.lnext(restrictions, it) {
		q := singleRestrictionToQual(it, node, cinfos)

		if q != nil {
			qualsList.Append(q)
		} else {
			// we failed to handle this restriction
			unhandledRestrictions++
		}

	}
	log.Printf("[TRACE] RestrictionsToQuals: converted postgres restrictions protobuf quals")
	for _, q := range qualsList.Quals {
		log.Printf("[TRACE] %s", grpc.QualToString(q))
	}
	if unhandledRestrictions > 0 {
		log.Printf("[WARN] RestrictionsToQuals: failed to convert %d %s to quals",
			unhandledRestrictions,
			pluralize.NewClient().Pluralize("restriction", unhandledRestrictions, false))
	}
	return qualsList, unhandledRestrictions
}

// build a protobuf qual from an OpExpr
func qualFromOpExpr(restriction *C.OpExpr, node *C.ForeignScanState, cinfos *conversionInfos) *proto.Qual {
	log.Printf("[TRACE] qualFromOpExpr")
	plan := (*C.ForeignScan)(unsafe.Pointer(node.ss.ps.plan))
	relids := C.bms_make_singleton(C.int(plan.scan.scanrelid))

	restriction = C.canonicalOpExpr(restriction, relids)
	if restriction == nil {
		log.Printf("[INFO] could not convert OpExpr to canonical form - NOT adding qual for OpExpr")
		return nil
	}

	left := (*C.Var)(C.list_nth(restriction.args, 0))
	right := C.list_nth(restriction.args, 1)

	// Do not add it if it either contains a mutable function, or makes self references in the right hand side.
	if C.contain_volatile_functions((*C.Node)(right)) || C.bms_is_subset(relids, C.pull_varnos(nil, (*C.Node)(right))) {
		log.Printf("[TRACE] restriction either contains a mutable function, or makes self references in the right hand side - NOT adding qual for OpExpr")
		return nil
	}

	var arrayIndex = int(left.varattno - 1)
	ci := cinfos.get(arrayIndex)
	if ci == nil {
		log.Printf("[WARN] failed to convert qual value - could not get conversion info for attribute %d", arrayIndex)
		return nil
	}

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

	log.Printf("[TRACE] qualFromOpExpr returning %v", qual)
	return qual
}

// build a protobuf qual from a Var - this converts to a simple boolean qual where column=true
func qualFromVar(arg *C.Var, node *C.ForeignScanState, cinfos *conversionInfos) *proto.Qual {
	column := columnFromVar(arg, cinfos)
	// if we failed to get a column we cannot create a qual
	if column == "" {
		log.Printf("[WARN] qualFromVar failed to get column from variable %v", arg)
		return nil
	}

	return &proto.Qual{
		FieldName: column,
		Operator:  &proto.Qual_StringValue{StringValue: "="},
		Value:     &proto.QualValue{Value: &proto.QualValue_BoolValue{BoolValue: true}},
	}
}

func qualFromScalarOpExpr(restriction *C.ScalarArrayOpExpr, node *C.ForeignScanState, cinfos *conversionInfos) *proto.Qual {
	plan := (*C.ForeignScan)(unsafe.Pointer(node.ss.ps.plan))
	relids := C.bms_make_singleton(C.int(plan.scan.scanrelid))

	restriction = C.canonicalScalarArrayOpExpr(restriction, relids)

	if restriction == nil {
		log.Printf("[WARN] could not convert OpExpr to canonical form - NOT adding qual for OpExpr")
		return nil
	}

	left := (*C.Var)(C.list_nth(restriction.args, 0))
	right := C.list_nth(restriction.args, 1)

	// Do not add it if it either contains a mutable function, or makes self references in the right hand side.
	if C.contain_volatile_functions((*C.Node)(right)) || C.bms_is_subset(relids, C.pull_varnos(nil, (*C.Node)(right))) {
		log.Printf("[TRACE] restriction either contains a mutable function, or makes self references in the right hand side - NOT adding qual for OpExpr")
		return nil
	}

	var arrayIndex = int(left.varattno - 1)
	ci := cinfos.get(arrayIndex)
	if ci == nil {
		log.Printf("[WARN]] failed to convert qual value - could not get conversion info for attribute %d", arrayIndex)
		return nil
	}

	qualValue, err := getQualValue(right, node, ci)
	if err != nil {
		log.Printf("[WARN] failed to convert qual value; %v", err)
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

// build a protobuf qual from a NullTest
func qualFromNullTest(restriction *C.NullTest, node *C.ForeignScanState, cinfos *conversionInfos) *proto.Qual {
	if C.fdw_nodeTag(restriction.arg) != C.T_Var {
		return nil
	}

	arg := (*C.Var)(unsafe.Pointer(restriction.arg))
	if arg.varattno < 1 {
		return nil
	}

	var operatorName string
	if restriction.nulltesttype == C.IS_NULL {
		operatorName = quals.QualOperatorIsNull
	} else {
		operatorName = quals.QualOperatorIsNotNull
	}

	// try to get the column
	column := columnFromVar(arg, cinfos)
	// if we failed to get a column we cannot create a qual
	if column == "" {
		log.Printf("[WARN] qualFromNullTest failed to get column from variable %v", arg)
		return nil
	}

	qual := &proto.Qual{
		FieldName: column,
		Operator:  &proto.Qual_StringValue{StringValue: operatorName},
		Value:     nil,
	}
	return qual
}

// build a protobuf qual from a BoolTest
func qualFromBooleanTest(restriction *C.BooleanTest, node *C.ForeignScanState, cinfos *conversionInfos) *proto.Qual {
	arg := restriction.arg
	if C.fdw_nodeTag(arg) != C.T_Var {
		return nil
	}

	// try to get the column
	variable := (*C.Var)(unsafe.Pointer(arg))
	column := columnFromVar(variable, cinfos)
	// if we failed to get a column we cannot create a qual
	if column == "" {
		log.Printf("[WARN] qualFromBooleanTest failed to get column from variable %v", variable)
		return nil
	}

	// now populate the operator
	operatorName := ""
	switch restriction.booltesttype {
	case C.IS_TRUE:
		operatorName = "="
	case C.IS_NOT_TRUE, C.IS_FALSE:
		operatorName = "<>"
	default:
		return nil
	}

	qual := &proto.Qual{
		FieldName: column,
		Operator:  &proto.Qual_StringValue{StringValue: operatorName},
		Value:     &proto.QualValue{Value: &proto.QualValue_BoolValue{BoolValue: true}},
	}

	return qual
}

// convert a boolean expression into a qual
// currently we support simple expressions like NOT column
// we also support col=X OR col=Y OR col=Z which is converted into col IN (X, Y, Z)
func qualFromBoolExpr(restriction *C.BoolExpr, node *C.ForeignScanState, cinfos *conversionInfos) *proto.Qual {
	// See https://doxygen.postgresql.org/primnodes_8h.html#a27f637bf3e2c33cc8e48661a8864c7af for the list
	boolExprNames := []string{"AND" /* 0 */, "OR" /* 1 */, "NOT" /* 2 */}
	log.Printf("[TRACE] qualFromBoolExpr op is %s with %d children", boolExprNames[restriction.boolop], C.list_length(restriction.args))

	arg := C.cellGetExpr(C.list_head(restriction.args))
	// NOTE this handles boolean expression with a single argument and a NOT operato
	if restriction.args.length == 1 || restriction.boolop == C.NOT_EXPR && C.fdw_nodeTag(arg) == C.T_Var {

		// try to get the column from the variable
		variable := C.cellGetVar(C.list_head(restriction.args))
		column := columnFromVar(variable, cinfos)
		// if we failed to get a column we cannot create a qual
		if column == "" {
			log.Printf("[WARN] qualFromBoolExpr failed to get column from variable %v", arg)
			return nil
		}

		return &proto.Qual{
			FieldName: column,
			Operator:  &proto.Qual_StringValue{StringValue: "<>"},
			Value:     &proto.QualValue{Value: &proto.QualValue_BoolValue{BoolValue: true}},
		}
	}

	// NOTE This handles queries of the form: WHERE column='...' OR column='...' OR column='...'
	// This is equivalent to: column IN ('...', '...', '...')
	if restriction.boolop == C.OR_EXPR {
		qualsForOrMembers := make([]*proto.Qual, 0, restriction.args.length)
		for it := C.list_head(restriction.args); it != nil; it = C.lnext(restriction.args, it) {
			// `it` is each part of the OR
			q := singleRestrictionToQual(it, node, cinfos) // reuse code that understands each part
			log.Printf("[TRACE] qualFromBoolExpr: OR member %s", q)
			if q == nil { // couldn't turn one part of the OR into a qual, so the entire OR can't be handled either
				log.Printf("[TRACE] qualFromBoolExpr can't convert OR to IN, part of OR can't be translated to qual")
				return nil
			}

			qualsForOrMembers = append(qualsForOrMembers, q)
		}

		log.Printf("[TRACE] qualFromBoolExpr: all OR exprs %s", qualsForOrMembers)

		// now check that they all target the same column and have operation EQUALS
		for _, part := range qualsForOrMembers {
			operator := part.Operator.(*proto.Qual_StringValue)
			// if an OR part isn't column='...', then we don't know how to handle that case so break out
			if part.FieldName != qualsForOrMembers[0].FieldName || operator.StringValue != "=" {
				log.Printf("[TRACE] qualFromBoolExpr can't convert OR to IN, not all OR refers to same column with ==")
				return nil
			}
		}
		log.Printf("[TRACE] all OR parts are == against same column %s, converting to IN", qualsForOrMembers[0].FieldName)

		// finally gather the values from each OR member clause
		qualValues := make([]*proto.QualValue, 0, len(qualsForOrMembers))
		for _, qual := range qualsForOrMembers {
			qualValues = append(qualValues, qual.Value)
		}

		// Build and return a single Qual:
		// * FieldName = the same one as in all the OR members
		// * Operator = EQUALS, the same one as in all the members
		// * Value = a List composed of the value of every OR member
		return &proto.Qual{
			FieldName: qualsForOrMembers[0].FieldName,
			Operator:  &proto.Qual_StringValue{StringValue: "="},
			Value: &proto.QualValue{
				Value: &proto.QualValue_ListValue{
					ListValue: &proto.QualValueList{
						Values: qualValues,
					},
				},
			},
		}
	}

	return nil
}

func columnFromVar(variable *C.Var, cinfos *conversionInfos) string {
	var arrayIndex = int(variable.varattno - 1)
	if arrayIndex < 0 {
		log.Printf("[WARN] columnFromVar failed - index %d, returning empty string", arrayIndex)
		return ""
	}
	ci := cinfos.get(arrayIndex)
	if ci == nil {
		log.Printf("[WARN] columnFromVar failed - could not get conversion info for index %d, returning empty string", arrayIndex)
		return ""
	}
	if ci.attrname == nil {
		log.Printf("[WARN] columnFromVar failed - conversion info for index %d has no attrname, returning empty string", arrayIndex)
		return ""
	}

	return C.GoString(ci.attrname)
}

func getQualValue(right unsafe.Pointer, node *C.ForeignScanState, ci *C.ConversionInfo) (*proto.QualValue, error) {
	log.Printf("[TRACE] getQualValue")
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
		log.Printf("[TRACE] getQualValue T_Const qual, value %v", value)
	case C.T_Param:
		paramQual := (*C.Param)(right)
		typeOid = paramQual.paramtype
		exprState := C.ExecInitExpr(valueExpression, (*C.PlanState)(unsafe.Pointer(node)))
		econtext := node.ss.ps.ps_ExprContext
		value = C.ExecEvalExpr(exprState, econtext, &isNull)
		log.Printf("[TRACE] getQualValue T_Param qual, value %v", value)
	case C.T_OpExpr:
		/* T_OpExpr may be something like
		   where date_time_column > current_timestamp - interval '1 hr'
		or
		   select * from
		     aws_ec2_instance as i,
				jsonb_array_elements(i.block_device_mappings) as b,
				aws_ebs_volume as v
			  where
				v.volume_id = b -> 'Ebs'  ->> 'VolumeId';

		evaluating jsonb_array_elements causes a crash, so exclude these quals from the evaluation
		*/

		opExprQual := (*C.OpExpr)(right)

		log.Printf("[TRACE] getQualValue T_OpExpr qual opno %d, opfuncid %d", opExprQual.opno, opExprQual.opfuncid)

		// TACTICAL we know that trying to evaluate a qual deriving from a jsonb_array_elements function call causes
		// ExecEvalExpr to crash - so skip evaluation in that case
		if opExprQual.opfuncid == 3214 {
			log.Printf("[TRACE] skipping OpExpr evaluation as opfuncid 3214 (jsonb_array_elements) is not supported)")
			return nil, fmt.Errorf("QualDefsToQuals: unsupported qual value (type %s, opfuncid %d), skipping\n", C.GoString(C.tagTypeToString(C.fdw_nodeTag(valueExpression))), opExprQual.opfuncid)
		}
		typeOid = opExprQual.opresulttype
		exprState := C.ExecInitExpr(valueExpression, (*C.PlanState)(unsafe.Pointer(node)))
		econtext := node.ss.ps.ps_ExprContext
		value = C.ExecEvalExpr(exprState, econtext, &isNull)
		log.Printf("[TRACE] getQualValue T_OpExpr qual, value %v, isNull %v", value, isNull)
	default:
		return nil, fmt.Errorf("QualDefsToQuals: unsupported qual value (type %s), skipping\n", C.GoString(C.tagTypeToString(C.fdw_nodeTag(valueExpression))))
	}

	var qualValue *proto.QualValue
	if isNull {
		log.Printf("[TRACE] qualDef.isnull=true - returning qual with nil value")
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

func datumToQualValue(datum C.Datum, typeOid C.Oid, cinfo *C.ConversionInfo) (result *proto.QualValue, err error) {
	/* we support these postgres column types (see sqlTypeForColumnType):
	 bool
	 bigint
	 double precision
	 text
	 inet
	 cidr
	 jsonb
	 timestamp

	so we must handle quals of all these types

	*/
	result = &proto.QualValue{}
	switch typeOid {
	case C.TEXTOID, C.VARCHAROID:
		result.Value = &proto.QualValue_StringValue{StringValue: C.GoString(C.datumString(datum, cinfo))}
	case C.INETOID, C.CIDROID:
		// handle zero value - return nil
		if datum == 0 {
			break
		}
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
		var timestamp *timestamp.Timestamp
		timestamp, err := PgTimeToTimestamp(pgts)
		if err != nil {
			break
		}
		result.Value = &proto.QualValue_TimestampValue{TimestampValue: timestamp}
	case C.TIMESTAMPOID, C.TIMESTAMPTZOID:
		pgts := int64(C.datumTimestamp(datum, cinfo))
		var timestamp *timestamp.Timestamp
		timestamp, err := PgTimeToTimestamp(pgts)
		if err != nil {
			break
		}
		result.Value = &proto.QualValue_TimestampValue{TimestampValue: timestamp}
	case C.INT2OID:
		result.Value = &proto.QualValue_Int64Value{Int64Value: int64(C.datumInt16(datum, cinfo))}
	case C.INT4OID:
		result.Value = &proto.QualValue_Int64Value{Int64Value: int64(C.datumInt32(datum, cinfo))}
	case C.INT8OID:
		result.Value = &proto.QualValue_Int64Value{Int64Value: int64(C.datumInt64(datum, cinfo))}
	case C.FLOAT4OID:
		result.Value = &proto.QualValue_DoubleValue{DoubleValue: float64(C.datumFloat4(datum, cinfo))}
	case C.FLOAT8OID:
		result.Value = &proto.QualValue_DoubleValue{DoubleValue: float64(C.datumFloat8(datum, cinfo))}
	case C.BOOLOID:
		result.Value = &proto.QualValue_BoolValue{BoolValue: bool(C.datumBool(datum, cinfo))}
	case C.JSONBOID:
		var jsonbQualStr string
		// handle zero value
		if datum == 0 {
			jsonbQualStr = "0"
		} else {
			jsonbQual := C.datumJsonb(datum, cinfo)
			jsonbQualStr = C.GoString(C.JsonbToCStringIndent(nil, &(jsonbQual.root), -1))
		}
		result.Value = &proto.QualValue_JsonbValue{JsonbValue: jsonbQualStr}
	default:
		log.Printf("[INFO] datumToQualValue unknown typeoid %v ", typeOid)
		result, err = convertUnknown(datum, typeOid, cinfo)
	}
	return
}

func convertUnknown(datum C.Datum, typeOid C.Oid, cinfo *C.ConversionInfo) (*proto.QualValue, error) {
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

	return nil, fmt.Errorf("Unknown qual type %v", typeOid)
}

func datumArrayToQualValue(datum C.Datum, typeOid C.Oid, cinfo *C.ConversionInfo) (*proto.QualValue, error) {
	var result = &proto.QualValue{
		Value: &proto.QualValue_ListValue{
			ListValue: &proto.QualValueList{},
		},
	}
	// NOTE: we have seen the occurrence of a zero datum with an array typeoid - explicitly check for this
	if datum == 0 {
		return result, nil
	}

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
			log.Printf("[INFO] datumArrayToQualValue: successfully converted qual value %v", qualValue)

			if !qualValueArrayContainsValue(qualValues, qualValue) {
				log.Printf("[INFO] adding qual value %v", qualValue)
				qualValues = append(qualValues, qualValue)
			} else {
				log.Printf("[INFO] qual value array already contains an identical qual value %v", qualValue)
			}
		}
	}
	result.Value = &proto.QualValue_ListValue{
		ListValue: &proto.QualValueList{
			Values: qualValues,
		},
	}

	log.Printf("[TRACE] datumArrayToQualValue complete, returning array of %d quals values \n", len(qualValues))

	return result, nil
}

func qualValueArrayContainsValue(values []*proto.QualValue, newVal *proto.QualValue) bool {
	for _, existingVal := range values {
		if existingVal.Equals(newVal) {
			return true
		}
	}
	return false
}
