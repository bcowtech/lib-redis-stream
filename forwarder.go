package redis

type Forwarder struct {
	*Producer
}

func NewForwarder(opt *Options) (*Forwarder, error) {
	producer, err := NewProducer(opt)
	if err != nil {
		return nil, err
	}
	instance := &Forwarder{
		Producer: producer,
	}
	return instance, nil
}

func (f *Forwarder) Runner() *ForwarderRunner {
	return &ForwarderRunner{
		handle: f,
	}
}
