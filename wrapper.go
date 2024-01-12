package pmq_responder

import (
	"github.com/joe-at-startupmedia/posix_mq"
)

type QueueConfig posix_mq.QueueConfig

const (
	O_RDONLY = posix_mq.O_RDONLY
	O_WRONLY = posix_mq.O_WRONLY
	O_RDWR   = posix_mq.O_RDWR

	O_CLOEXEC  = posix_mq.O_CLOEXEC
	O_CREAT    = posix_mq.O_CREAT
	O_EXCL     = posix_mq.O_EXCL
	O_NONBLOCK = posix_mq.O_NONBLOCK
)

// GetFile gets the file on the OS where the queues are stored
func (config *QueueConfig) GetFile() string {
	return (*posix_mq.QueueConfig)(config).GetFile()
}

// NewMessageQueue returns an instance of the message queue given a QueueConfig.
func NewMessageQueue(config *QueueConfig) (*posix_mq.MessageQueue, error) {
	return posix_mq.NewMessageQueue((*posix_mq.QueueConfig)(config))
}

type BidirectionalQueue struct {
	mqRqst *posix_mq.MessageQueue
	mqResp *posix_mq.MessageQueue
}

func (bdr *BidirectionalQueue) Close() error {
	if err := bdr.mqRqst.Close(); err != nil {
		return err
	}
	return bdr.mqResp.Close()
}

func (bdr *BidirectionalQueue) Unlink() error {
	if err := bdr.mqRqst.Unlink(); err != nil {
		return err
	}
	return bdr.mqResp.Unlink()
}
