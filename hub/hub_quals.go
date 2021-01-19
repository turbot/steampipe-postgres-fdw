package hub

import (
	"log"

	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

func (h *Hub) buildQualMap(quals []*proto.Qual) (map[string]*proto.Quals, error) {
	qualMap := make(map[string]*proto.Quals)

	for _, qual := range quals {
		// convert the qual value from cty value to a protobuf 'QualValue' type
		columnQuals, ok := qualMap[qual.FieldName]
		if ok {
			columnQuals.Quals = append(columnQuals.Quals, qual)
		} else {
			columnQuals = &proto.Quals{Quals: []*proto.Qual{qual}}
		}
		qualMap[qual.FieldName] = columnQuals
	}

	if len(qualMap) > 0 {

		log.Printf("[INFO] Quals %s", grpc.QualMapToString(qualMap))
	} else {
		log.Println("[INFO] no quals")
	}
	return qualMap, nil
}
