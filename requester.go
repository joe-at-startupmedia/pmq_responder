package pmq_responder

import (
	"fmt"
	"github.com/joe-at-startupmedia/pmq_responder/protos"
	"google.golang.org/protobuf/proto"
	"time"

	"github.com/joe-at-startupmedia/posix_mq"
)

type MqRequester BidirectionalQueue

func NewRequester(config *QueueConfig, owner *Ownership) (*MqRequester, error) {
	requester, err := openQueueForRequester(config, owner, "rqst")
	if err != nil {
		return nil, err
	}

	responder, err := openQueueForRequester(config, owner, "resp")

	mqs := MqRequester{
		requester,
		responder,
	}

	return &mqs, err
}

func openQueueForRequester(config *QueueConfig, owner *Ownership, postfix string) (*posix_mq.MessageQueue, error) {
	if config.Flags == 0 {
		config.Flags = posix_mq.O_RDWR
	}
	return NewMessageQueueWithOwnership(*config, owner, postfix)
}

func (mqs *MqRequester) Request(data []byte, priority uint) error {
	return mqs.mqRqst.Send(data, priority)
}

func (mqs *MqRequester) RequestUsingMqRequest(req *MqRequest, priority uint) error {
	if !req.HasId() {
		req.SetId()
	}
	data, err := proto.Marshal(req.AsProtobuf())
	if err != nil {
		return fmt.Errorf("marshaling error: %w", err)
	}
	return mqs.Request(data, priority)
}

func (mqs *MqRequester) RequestUsingProto(req *proto.Message, priority uint) error {
	data, err := proto.Marshal(*req)
	if err != nil {
		return fmt.Errorf("marshaling error: %w", err)
	}
	return mqs.Request(data, priority)
}

func (mqs *MqRequester) WaitForResponse(duration time.Duration) ([]byte, uint, error) {
	return mqs.mqResp.TimedReceive(duration)
}

func (mqs *MqRequester) WaitForMqResponse(duration time.Duration) (*MqResponse, uint, error) {
	pbm, prio, err := mqs.WaitForProto(&protos.Response{}, duration)
	mqResp, err := ProtoMessageToMqResponse(pbm)
	if err != nil {
		return nil, 0, err
	}
	return mqResp, prio, err
}

func (mqs *MqRequester) WaitForProto(pbm proto.Message, duration time.Duration) (*proto.Message, uint, error) {
	data, prio, err := mqs.mqResp.TimedReceive(duration)
	if err != nil {
		return nil, 0, err
	}
	err = proto.Unmarshal(data, pbm)
	return &pbm, prio, err
}

func (mqs *MqRequester) CloseRequester() error {
	return (*BidirectionalQueue)(mqs).Close()
}

func (mqs *MqRequester) UnlinkRequester() error {
	return (*BidirectionalQueue)(mqs).Unlink()
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
