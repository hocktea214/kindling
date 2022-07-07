package receiver

type Receiver interface {
	// Type returns the type of the analyzer
	Type() string
	// Start initializes the receiver and start to receive events.
	Start() error
	// Shutdown closes the receiver and stops receiving events.
	// Note receiver should not shutdown other components though it holds a reference
	Shutdown() error
}
