package pmq_responder

import (
	"errors"
	"fmt"
	"github.com/joe-at-startupmedia/pmq_responder/protos"
	"github.com/joe-at-startupmedia/posix_mq"
	"google.golang.org/protobuf/proto"
	"syscall"
	"time"
)

type ResponderCallback func(msq []byte) (processed []byte, err error)

type ResponderMqRequestCallback func(mqs *MqRequest) (mqr *MqResponse, err error)

type ResponderFromProtoMessageCallback func(mqs *proto.Message) (processed []byte, err error)

type MqResponder BidirectionalQueue

func NewResponder(config *QueueConfig, owner *Ownership) (*MqResponder, error) {

	requester, err := openQueueForResponder(config, owner, "rqst")
	if err != nil {
		return nil, err
	}

	responder, err := openQueueForResponder(config, owner, "resp")
	if err != nil {
		return nil, err
	}

	mqr := MqResponder{
		requester,
		responder,
	}

	return &mqr, nil
}

func openQueueForResponder(config *QueueConfig, owner *Ownership, postfix string) (*posix_mq.MessageQueue, error) {

	if config.Flags == 0 {
		config.Flags = O_RDWR | O_CREAT | O_NONBLOCK
	}
	return NewMessageQueueWithOwnership(*config, owner, postfix)
}

// HandleMqRequest provides a concrete implementation of HandleRequestFromProto using the local MqRequest type
func (mqr *MqResponder) HandleMqRequest(requestProcessor ResponderMqRequestCallback) error {

	return mqr.HandleRequestFromProto(&protos.Request{}, func(pbm *proto.Message) (processed []byte, err error) {

		mqReq, err := ProtoMessageToMqRequest(pbm)
		if err != nil {
			return nil, err
		}

		mqResp, err := requestProcessor(mqReq)
		if err != nil {
			return nil, err
		}

		data, err := proto.Marshal(mqResp.AsProtobuf())

		if err != nil {
			return nil, fmt.Errorf("marshaling error: %w", err)
		}

		return data, nil
	})
}

// HandleRequestFromProto used to process arbitrary protobuf messages using a callback
func (mqr *MqResponder) HandleRequestFromProto(protocMsg proto.Message, msgHandler ResponderFromProtoMessageCallback) error {
	msg, _, err := mqr.mqRqst.Receive()
	if err != nil {
		//EAGAIN simply means the queue is empty when O_NONBLOCK is set
		// @TODO detect if O_NONBLOCK was set
		if errors.Is(err, syscall.EAGAIN) {
			return nil
		}
		return err
	}

	err = proto.Unmarshal(msg, protocMsg)
	if err != nil {
		return fmt.Errorf("unmarshaling error: %w", err)
	}

	processed, err := msgHandler(&protocMsg)
	if err != nil {
		return err
	}

	return mqr.mqResp.Send(processed, 0)
}

func (mqr *MqResponder) HandleRequest(msgHandler ResponderCallback) error {
	return mqr.handleRequest(msgHandler, 0)
}

// HandleRequestWithLag used for testing purposes to simulate lagging responder
func (mqr *MqResponder) HandleRequestWithLag(msgHandler ResponderCallback, lag int) error {
	return mqr.handleRequest(msgHandler, lag)
}

func (mqr *MqResponder) handleRequest(msgHandler ResponderCallback, lag int) error {
	msg, _, err := mqr.mqRqst.Receive()
	if err != nil {
		//EAGAIN simply means the queue is empty when O_NONBLOCK is set
		// @TODO detect if O_NONBLOCK was set
		if errors.Is(err, syscall.EAGAIN) {
			return nil
		}
		return err
	}
	processed, err := msgHandler(msg)
	if err != nil {
		return err
	}

	if lag > 0 {
		time.Sleep(time.Duration(lag) * time.Second)
	}

	err = mqr.mqResp.Send(processed, 0)
	return err
}

func (mqr *MqResponder) CloseResponder() error {
	return (*BidirectionalQueue)(mqr).Close()
}

func (mqr *MqResponder) UnlinkResponder() error {
	return (*BidirectionalQueue)(mqr).Unlink()
}

func CloseResponder(mqr *MqResponder) error {
	if mqr != nil {
		return mqr.CloseResponder()
	}
	return fmt.Errorf("pointer reference is nil")
}

func UnlinkResponder(mqr *MqResponder) error {
	if mqr != nil {
		return mqr.UnlinkResponder()
	}
	return fmt.Errorf("pointer reference is nil")
}
