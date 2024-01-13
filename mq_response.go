package pmq_responder

import (
	"github.com/joe-at-startupmedia/pmq_responder/protos"
)

type MqResponse protos.Response

func (mqr *MqResponse) AsProtobuf() *protos.Response {
	return (*protos.Response)(mqr)
}

func (mqr *MqResponse) PrepareFromRequest(mqs *MqRequest) *MqResponse {
	mqr.RequestId = mqs.Id
	return mqr
}

func ToMqResponse(mqr *protos.Response) *MqResponse {
	return (*MqResponse)(mqr)
}
