package hub

import (
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
)

func (h *Hub) buildQualMap(quals *proto.Quals) (map[string]*proto.Quals, error) {
	qualMap := make(map[string]*proto.Quals)

	for _, qual := range quals.Quals {
		if qual == nil {
			continue
		}

		columnQuals, ok := qualMap[qual.FieldName]
		if ok {
			columnQuals.Append(qual)
		} else {
			columnQuals = &proto.Quals{Quals: []*proto.Qual{qual}}
		}
		qualMap[qual.FieldName] = columnQuals
	}
	return qualMap, nil
}
