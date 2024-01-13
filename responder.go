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

type ResponderProtoCallback func(mqs *MqRequest) (mqr *MqResponse, err error)

type MqResponder BidirectionalQueue

func NewResponder(config QueueConfig, owner *Ownership) (*MqResponder, error) {

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

func openQueueForResponder(config QueueConfig, owner *Ownership, postfix string) (*posix_mq.MessageQueue, error) {

	if config.Flags == 0 {
		config.Flags = O_RDWR | O_CREAT | O_NONBLOCK
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
	if owner != nil {
		err = owner.ApplyPermissions(&config)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Could not apply permissions %s: %-v", config.GetFile(), err))
		}
	}
	return messageQueue, nil
}

func (mqr *MqResponder) HandleRequestProto(msgHandler ResponderProtoCallback) error {
	msg, _, err := mqr.mqRqst.Receive()
	if err != nil {
		//EAGAIN simply means the queue is empty when O_NONBLOCK is set
		// @TODO detect if O_NONBLOCK was set
		if errors.Is(err, syscall.EAGAIN) {
			return nil
		}
		return err
	}

	newRequest := &protos.Request{}
	err = proto.Unmarshal(msg, newRequest)
	if err != nil {
		return fmt.Errorf("unmarshaling error: %w", err)
	}

	processed, err := msgHandler(ToMqRequest(newRequest))
	if err != nil {
		return err
	}
	data, err := proto.Marshal(processed.AsProtobuf())
	if err != nil {
		return fmt.Errorf("marshaling error: %w", err)
	}
	err = mqr.mqResp.Send(data, 0)
	return err
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
