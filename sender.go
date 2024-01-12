package pmq_responder

import (
	"errors"
	"fmt"
	"github.com/joe-at-startupmedia/posix_mq"
	"time"
)

type MqSender struct {
	mqSend *posix_mq.MessageQueue
	mqResp *posix_mq.MessageQueue
}

func NewSender(config QueueConfig, owner *Ownership) (*MqSender, error) {
	sender, err := openQueueForSender(config, owner, "send")
	if err != nil {
		return nil, err
	}

	responder, err := openQueueForSender(config, owner, "resp")

	mqs := MqSender{
		sender,
		responder,
	}

	return &mqs, err
}

func openQueueForSender(config QueueConfig, owner *Ownership, postfix string) (*posix_mq.MessageQueue, error) {
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

func (mqs *MqSender) Send(data []byte, priority uint) error {
	return mqs.mqSend.Send(data, priority)
}

func (mqs *MqSender) WaitForResponse(duration time.Duration) ([]byte, uint, error) {
	return mqs.mqResp.TimedReceive(duration)
}

func (mqs *MqSender) Close() error {
	if err := mqs.mqSend.Close(); err != nil {
		return err
	}
	return mqs.mqResp.Close()
}
