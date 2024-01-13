package pmq_responder

import (
	"errors"
	"fmt"
	"github.com/joe-at-startupmedia/pmq_responder/protos"
	"google.golang.org/protobuf/proto"
	"time"

	"github.com/joe-at-startupmedia/posix_mq"
)

type MqRequester BidirectionalQueue

func NewRequester(config QueueConfig, owner *Ownership) (*MqRequester, error) {
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

func openQueueForRequester(config QueueConfig, owner *Ownership, postfix string) (*posix_mq.MessageQueue, error) {
	if config.Flags == 0 {
		config.Flags = posix_mq.O_RDWR
	}
	config.Name = fmt.Sprintf("%s_%s", config.Name, postfix)
	var (
		messageQueue *posix_mq.MessageQueue
		err          error
	)
	if owner != nil && owner.IsValid() {
		config.Mode = 0660
		messageQueue, err = NewMessageQueue(&config)

	} else {
		config.Mode = 0666
		messageQueue, err = NewMessageQueue(&config)
	}
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Could not create message queue %s: %-v", config.GetFile(), err))
	}
	return messageQueue, nil
}

func (mqs *MqRequester) Request(data []byte, priority uint) error {
	return mqs.mqRqst.Send(data, priority)
}

func (mqs *MqRequester) RequestProto(req *MqRequest, priority uint) error {
	if !req.HasId() {
		req.SetId()
	}
	data, err := proto.Marshal(req.AsProtobuf())
	if err != nil {
		return fmt.Errorf("marshaling error: %w", err)
	}
	return mqs.mqRqst.Send(data, priority)
}

func (mqs *MqRequester) WaitForResponse(duration time.Duration) ([]byte, uint, error) {
	return mqs.mqResp.TimedReceive(duration)
}

func (mqs *MqRequester) WaitForResponseProto(duration time.Duration) (*MqResponse, uint, error) {
	data, prio, err := mqs.mqResp.TimedReceive(duration)
	if err != nil {
		return nil, 0, err
	}
	newResponse := &protos.Response{}
	err = proto.Unmarshal(data, newResponse)
	return ToMqResponse(newResponse), prio, err
}

func (mqs *MqRequester) CloseRequester() error {
	return (*BidirectionalQueue)(mqs).Close()
}

func (mqs *MqRequester) UnlinkRequester() error {
	return (*BidirectionalQueue)(mqs).Unlink()
}
