package receiver

import (
	"errors"

	"github.com/hashicorp/go-multierror"
	"go.uber.org/zap"
)

type Manager struct {
	allReceivers []Receiver
}

func NewManager(receivers ...Receiver) (*Manager, error) {
	if len(receivers) == 0 {
		return nil, errors.New("no receivers found, but must provide at least one receiver")
	}
	allReceivers := make([]Receiver, 0)
	allReceivers = append(allReceivers, receivers...)

	return &Manager{
		allReceivers: allReceivers,
	}, nil
}

func (m *Manager) StartAll(logger *zap.Logger) error {
	for _, receiver := range m.allReceivers {
		logger.Sugar().Infof("Starting receiver [%s]", receiver.Type())
		err := receiver.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) ShutdownAll(logger *zap.Logger) error {
	var retErr error = nil
	for _, receiver := range m.allReceivers {
		logger.Sugar().Infof("Shutdown receiver [%s]", receiver.Type())
		err := receiver.Shutdown()
		if err != nil {
			retErr = multierror.Append(retErr, err)
		}
	}
	return retErr
}
