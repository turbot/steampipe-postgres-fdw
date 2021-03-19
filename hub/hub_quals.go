package hub

import (
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

func (h *Hub) buildQualMap(quals []*proto.Qual) (map[string]*proto.Quals, error) {
	qualMap := make(map[string]*proto.Quals)

	for _, qual := range quals {
		if qual == nil {
			continue
		}
		// convert the qual value from cty value to a protobuf 'QualValue' type
		columnQuals, ok := qualMap[qual.FieldName]
		if ok {
			columnQuals.Quals = append(columnQuals.Quals, qual)
		} else {
			columnQuals = &proto.Quals{Quals: []*proto.Qual{qual}}
		}
		qualMap[qual.FieldName] = columnQuals
	}
	return qualMap, nil
}
