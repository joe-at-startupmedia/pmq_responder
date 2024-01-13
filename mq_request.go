package pmq_responder

import (
	"github.com/google/uuid"
	"github.com/joe-at-startupmedia/pmq_responder/protos"
)

type MqRequest protos.Request

func (mqr *MqRequest) HasId() bool {
	return len(mqr.Id) > 0
}

func (mqr *MqRequest) SetId() {
	mqr.Id = uuid.NewString()
}

func (mqr *MqRequest) AsProtobuf() *protos.Request {
	return (*protos.Request)(mqr)
}

func ToMqRequest(mqr *protos.Request) *MqRequest {
	return (*MqRequest)(mqr)
}
