package pmq_responder

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/joe-at-startupmedia/pmq_responder/protos"
	"google.golang.org/protobuf/proto"
)

type MqRequest protos.Request

func (mqr *MqRequest) HasId() bool {
	return len(mqr.Id) > 0
}

func (mqr *MqRequest) SetId() {
	mqr.Id = uuid.NewString()
}

// AsProtobuf used to convert the local type equivalent (MqRequest)
// back to its protobuf instance
func (mqr *MqRequest) AsProtobuf() *protos.Request {
	return (*protos.Request)(mqr)
}

// ProtoRequestToMqRequest used to convert the protobuf to the local
// type equivalent (MqRequest) for leveraging instance methods
func ProtoRequestToMqRequest(mqr *protos.Request) *MqRequest {
	return (*MqRequest)(mqr)
}

// ProtoMessageToMqRequest used to convert a generic protobuf message to an MsRequest
func ProtoMessageToMqRequest(pbm *proto.Message) (*MqRequest, error) {
	msg, err := proto.Marshal(*pbm)
	if err != nil {
		return nil, fmt.Errorf("marshaling error: %w", err)
	}
	mqReq := protos.Request{}
	err = proto.Unmarshal(msg, &mqReq)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling error: %w", err)
	}
	return ProtoRequestToMqRequest(&mqReq), nil
}
