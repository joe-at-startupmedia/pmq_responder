package pmq_responder

import (
	"fmt"
	"github.com/joe-at-startupmedia/pmq_responder/protos"
	"google.golang.org/protobuf/proto"
	"time"

	"github.com/joe-at-startupmedia/posix_mq"
)

type MqRequester BidirectionalQueue

func NewRequester(config *QueueConfig, owner *Ownership) *MqRequester {
	requester, errRqst := openQueueForRequester(config, owner, "rqst")

	responder, errResp := openQueueForRequester(config, owner, "resp")

	mqs := MqRequester{
		requester,
		errRqst,
		responder,
		errResp,
	}

	return &mqs
}

func openQueueForRequester(config *QueueConfig, owner *Ownership, postfix string) (*posix_mq.MessageQueue, error) {
	if config.Flags == 0 {
		config.Flags = posix_mq.O_RDWR
	}
	return NewMessageQueueWithOwnership(*config, owner, postfix)
}

func (mqs *MqRequester) Request(data []byte) error {
	return mqs.MqRqst.Send(data, 0)
}

func (mqs *MqRequester) RequestUsingMqRequest(req *MqRequest) error {
	if !req.HasId() {
		req.SetId()
	}
	pbm := proto.Message(req.AsProtobuf())
	return mqs.RequestUsingProto(&pbm)
}

func (mqs *MqRequester) RequestUsingProto(req *proto.Message) error {
	data, err := proto.Marshal(*req)
	if err != nil {
		return fmt.Errorf("marshaling error: %w", err)
	}
	return mqs.Request(data)
}

func (mqs *MqRequester) WaitForResponse(duration time.Duration) ([]byte, error) {
	msg, _, err := mqs.MqResp.TimedReceive(duration)
	return msg, err
}

func (mqs *MqRequester) WaitForMqResponse(duration time.Duration) (*MqResponse, error) {
	mqResp := &protos.Response{}
	_, err := mqs.WaitForProto(mqResp, duration)
	if err != nil {
		return nil, err
	}
	return ProtoResponseToMqResponse(mqResp), err
}

func (mqs *MqRequester) WaitForProto(pbm proto.Message, duration time.Duration) (*proto.Message, error) {
	data, _, err := mqs.MqResp.TimedReceive(duration)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(data, pbm)
	return &pbm, err
}

func (mqs *MqRequester) CloseRequester() error {
	return (*BidirectionalQueue)(mqs).Close()
}

func (mqs *MqRequester) UnlinkRequester() error {
	return (*BidirectionalQueue)(mqs).Unlink()
}

func (mqr *MqRequester) HasErrors() bool {
	return (*BidirectionalQueue)(mqr).HasErrors()
}

func (mqr *MqRequester) Error() error {
	return (*BidirectionalQueue)(mqr).Error()
}

func CloseRequester(mqr *MqRequester) error {
	if mqr != nil {
		return mqr.CloseRequester()
	}
	return fmt.Errorf("pointer reference is nil")
}

func UnlinkRequester(mqr *MqRequester) error {
	if mqr != nil {
		return mqr.UnlinkRequester()
	}
	return fmt.Errorf("pointer reference is nil")
}
