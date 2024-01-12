package pmq_responder

import (
	"errors"
	"fmt"
	"github.com/joe-at-startupmedia/posix_mq"
	"syscall"
	"time"
)

type ResponderCallback func(msq []byte) (processed []byte, err error)

type MqResponder struct {
	mqSend *posix_mq.MessageQueue
	mqResp *posix_mq.MessageQueue
}

func NewResponder(config QueueConfig, owner *Ownership) (*MqResponder, error) {

	sender, err := openQueueForResponder(config, owner, "send")
	if err != nil {
		return nil, err
	}

	responder, err := openQueueForResponder(config, owner, "resp")
	if err != nil {
		return nil, err
	}

	mqr := MqResponder{
		sender,
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

func (mqr *MqResponder) HandleRequest(msgHandler ResponderCallback) error {
	return mqr.handleRequest(msgHandler, 0)
}

func (mqr *MqResponder) HandleRequestWithLag(msgHandler ResponderCallback, lag int) error {
	return mqr.handleRequest(msgHandler, lag)
}

func (mqr *MqResponder) handleRequest(msgHandler ResponderCallback, lag int) error {
	msg, _, err := mqr.mqSend.Receive()
	if err != nil {
		//EAGAIN simply means the queue is empty when O_NONBLOCK is set
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
	if err := mqr.mqSend.Close(); err != nil {
		return err
	}
	return mqr.mqResp.Close()
}
