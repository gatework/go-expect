package expect

// inputMailbox serializes the two receive consumers (Expect and SendAll).
// Queue mutation and retirement require inputGate; the sole pump owns events.
type inputMailbox struct {
	events      chan readEvent
	inputGate   chan struct{}
	inputReady  chan struct{}
	queued      []readEvent
	queuedBytes int
	retired     bool
}

func (m *inputMailbox) retire() {
	m.inputGate <- struct{}{}
	defer func() { <-m.inputGate }()
	m.retired = true
	m.queued = nil
	m.queuedBytes = 0
	// The owner joins the pump before retirement.
	for range m.events {
	}
}
func (m *inputMailbox) takeQueued() (readEvent, bool) {
	if len(m.queued) == 0 {
		return readEvent{}, false
	}
	event := m.queued[0]
	m.queued[0] = readEvent{}
	m.queued = nil
	m.queuedBytes = 0
	return event, true
}

func (m *inputMailbox) enqueue(event readEvent, limit int) error {
	if m.retired {
		return ErrClosed
	}
	if len(event.data) == 0 && event.err == nil {
		return nil
	}
	if m.queuedBytes+len(event.data) > limit {
		return ErrBufferLimit
	}
	if len(m.queued) == 0 {
		data := event.data
		if cap(data) > limit {
			data = make([]byte, len(event.data))
			copy(data, event.data)
		}
		m.queued = []readEvent{{data: data, err: event.err}}
		m.queuedBytes = len(data)
		return nil
	}
	queued := &m.queued[0]
	size := len(queued.data) + len(event.data)
	if size > cap(queued.data) {
		capacity := min(limit, max(size, 2*cap(queued.data)))
		data := make([]byte, size, capacity)
		copy(data, queued.data)
		copy(data[len(queued.data):], event.data)
		queued.data = data
	} else {
		queued.data = append(queued.data, event.data...)
	}
	if event.err != nil {
		queued.err = event.err
	}
	m.queuedBytes = size
	return nil
}

func (m *inputMailbox) signalInput() {
	select {
	case m.inputReady <- struct{}{}:
	default:
	}
}
