package pmq_responder

import (
	"fmt"
	"github.com/joe-at-startupmedia/pmq_responder/protos"
	"google.golang.org/protobuf/proto"
)

type MqResponse protos.Response

// AsProtobuf used to convert the local type equivalent (MqResponse)
// back to its protobuf instance
func (mqr *MqResponse) AsProtobuf() *protos.Response {
	return (*protos.Response)(mqr)
}

func (mqr *MqResponse) PrepareFromRequest(mqs *MqRequest) *MqResponse {
	mqr.RequestId = mqs.Id
	return mqr
}

// ProtoResponseToMqResponse used to convert the protobuf to the local
// type equivalent (MqResponse) for leveraging instance methods
func ProtoResponseToMqResponse(mqr *protos.Response) *MqResponse {
	return (*MqResponse)(mqr)
}

// ProtoMessageToMqResponse used to convert a generic protobuf message to an MsResponse
func ProtoMessageToMqResponse(pbm *proto.Message) (*MqResponse, error) {
	msg, err := proto.Marshal(*pbm)
	if err != nil {
		return nil, fmt.Errorf("marshaling error: %w", err)
	}
	mqRsp := protos.Response{}
	err = proto.Unmarshal(msg, &mqRsp)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling error: %w", err)
	}
	return ProtoResponseToMqResponse(&mqRsp), nil
}
