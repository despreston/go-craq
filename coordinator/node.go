package coordinator

import (
	"time"

	"github.com/despreston/go-craq/transport"
)

type node struct {
	rpc       transport.NodeClient
	connected bool
	last      time.Time // last successful ping
	address   string    // host and port
}

func (n *node) Connect() error {
	if err := n.rpc.Connect(n.address); err != nil {
		n.connected = false
		return err
	}
	n.connected = true
	return nil
}

func (n node) Address() string { return n.address }
