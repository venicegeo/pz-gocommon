package piazza

import ()

type IService interface {
	GetName() ServiceName
	GetAddress() string
}
