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

//func QualDefsToQuals(qualDefs *C.List, cinfos **C.ConversionInfo) []*proto.Qual {
//	var quals []*proto.Qual
//	if qualDefs == nil {
//		return quals
//	}
//	for it := qualDefs.head; it != nil; it = it.next {
//		var qualDef *C.FdwBaseQual
//		qualDef = C.cellGetBaseQual(it)
//		if qualDef.right_type == C.T_Const {
//			constDef := C.cellGetConstQual(it)
//			if qual, err := qualDefToQual(constDef, cinfos); err != nil {
//				log.Printf("[ERROR] failed to convert qual def to qual: %v", err)
//			} else {
//				quals = append(quals, qual)
//			}
//		} else {
//			log.Printf("[TRACE] QualDefsToQuals: non-const qual value (type %v), skipping\n", qualDef.right_type)
//		}
//
//	}
//	log.Printf("[TRACE] QualDefsToQuals: converted quals from postgres datums to protobuf quals")
//	for _, q := range quals {
//		log.Printf("[DEBUG] field '%s' operator '%s' value '%v'\n", q.FieldName, q.Operator, q.Value)
//	}
//	return quals
//}

//func qualDefToQual(qualDef *C.FdwConstQual, cinfos **C.ConversionInfo) (*proto.Qual, error) {
//	arrayIndex := qualDef.base.varattno - 1
//	operatorName := qualDef.base.opname
//	isArray := qualDef.base.isArray
//	useOr := qualDef.base.useOr
//	typeOid := qualDef.base.typeoid
//	value := qualDef.value
//	isNull := qualDef.isnull
//
//	log.Printf(`[TRACE] qualDefToQual: convert postgres qual to protobuf qual
//  arrayIndex: %d
//  operatorName: %s
//  isArray: %v
//  useOr: %v
//  typeOid: %v
//  isNull: %v
//  value %v`, arrayIndex, C.GoString(operatorName), isArray, useOr, typeOid, isNull, value)
//
//	ci := C.getConversionInfo(cinfos, C.int(arrayIndex))
//
//	column := C.GoString(ci.attrname)
//	var result *proto.QualValue
//	var err error
//	if isNull {
//		log.Printf("[TRACE] qualDef.isnull=true - returning qual with nil value")
//		result = nil
//	} else {
//		if typeOid == C.InvalidOid {
//			typeOid = ci.atttypoid
//		}
//		if result, err = datumToQualValue(value, typeOid, ci); err != nil {
//			return nil, err
//		}
//	}
//
//	if typeOid <= 0 {
//		typeOid = ci.atttypoid
//	}
//
//	log.Printf(`[TRACE] QUAL
//  fieldName: %s
//  operatorName: %s
//  value: %v
//`, C.GoString(ci.attrname), C.GoString(operatorName), result)
//
//	qual := &proto.Qual{
//		FieldName: column,
//		Operator:  &proto.Qual_StringValue{StringValue: C.GoString(operatorName)},
//		Value:     result,
//	}
//
//	//spew.Dump(qual)
//	return qual, nil
//
//}

func RestrictionsToQuals(restrictions *C.List, relids C.Relids, cinfos **C.ConversionInfo) []*proto.Qual {
	var quals []*proto.Qual
	if restrictions == nil {
		return quals
	}

	for it := restrictions.head; it != nil; it = it.next {
		restriction := C.cellGetExpr(it)

		switch C.fdw_nodeTag(restriction) {
		case C.T_OpExpr:
			quals = append(quals, qualFromOpExpr((*C.OpExpr)(unsafe.Pointer(restriction)), relids, cinfos))
			break
		case C.T_NullTest:
			//extractClauseFromNullTest(base_relids,				(NullTest *) node, quals);
			break
		case C.T_ScalarArrayOpExpr:
			//extractClauseFromScalarArrayOpExpr(base_relids,				(ScalarArrayOpExpr *) node,			quals);
			break
		case C.T_BooleanTest:
			//extractClauseFromBooleanTest(base_relids,				(BooleanTest *) node,			quals);
			break
		case C.T_BoolExpr:
			//extractClauseFromBooleanTest(base_relids,				(BooleanTest *) node,			quals);
			break
		}

	}
	log.Printf("[TRACE] QualDefsToQuals: converted quals from postgres datums to protobuff quals")
	for _, q := range quals {
		log.Printf("[WARN] field '%s' operator '%s' value '%v'\n", q.FieldName, q.Operator, q.Value)
	}
	return quals
}

func qualFromOpExpr(restriction *C.OpExpr, relids C.Relids, cinfos **C.ConversionInfo) *proto.Qual {
	log.Printf("[INFO] qualFromOpExpr rel %+v is member %v, %s", relids, C.bms_is_member(1, relids), C.GoString(C.nodeToString(unsafe.Pointer(restriction))))
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
	column := C.GoString(ci.attrname)
	operatorName := C.GoString(C.getOperatorString(restriction.opno))

	if C.fdw_nodeTag((*C.Expr)(right)) != C.T_Const {
		log.Printf("[INFO] QualDefsToQuals: non-const qual value (type %v), skipping\n", C.fdw_nodeTag((*C.Expr)(right)))
		return nil

	}

	constQual := (*C.Const)(right)

	typeOid := constQual.consttype
	var value C.Datum = constQual.constvalue
	isNull := constQual.constisnull

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
			log.Printf("[WARN] failed to convert datum to qual value: %v", err)
			return nil
		}
	}
	qual := &proto.Qual{
		FieldName: column,
		Operator:  &proto.Qual_StringValue{StringValue: operatorName},
		Value:     qualValue,
	}

	return qual
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
