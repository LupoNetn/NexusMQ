package broker

import (
	"errors"
)

var (
	ErrTopicAlreadyExists = errors.New("topic already exists")
	ErrTopicNotFound    = errors.New("topic not found")
)