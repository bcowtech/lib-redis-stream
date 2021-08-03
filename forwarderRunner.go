package redis

type ForwarderRunner struct {
	handle *Forwarder
}

func (r *ForwarderRunner) Start() {
	logger.Println("Started")
}

func (r *ForwarderRunner) Stop() {
	logger.Println("Stopping")
	r.handle.Close()
	logger.Println("Stopped")
}
