package broker

import (
	"errors"
)

var (
	ErrTopicAlreadyExists = errors.New("topic already exists")
	ErrTopicNotFound      = errors.New("topic not found")
	ErrPublishTimeout     = errors.New("publish timeout on one or more subscribers")
)