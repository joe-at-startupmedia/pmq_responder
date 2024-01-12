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
	return config.GetFile()
}

// NewMessageQueue returns an instance of the message queue given a QueueConfig.
func NewMessageQueue(config *QueueConfig) (*posix_mq.MessageQueue, error) {

	return posix_mq.NewMessageQueue((*posix_mq.QueueConfig)(config))
}