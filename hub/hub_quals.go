package hub

import (
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

func (h *Hub) buildQualMap(quals *proto.DbQuals) (map[string]*proto.DbQuals, error) {
	qualMap := make(map[string]*proto.DbQuals)

	for _, dbQual := range quals.Quals {
		if dbQual == nil {
			continue
		}
		qual := dbQual.GetQual()
		if qual == nil {
			continue
		}

		columnQuals, ok := qualMap[qual.FieldName]
		if ok {
			columnQuals.Append(dbQual)
		} else {
			columnQuals = &proto.DbQuals{Quals: []*proto.DbQual{dbQual}}
		}
		qualMap[qual.FieldName] = columnQuals
	}
	return qualMap, nil
}
