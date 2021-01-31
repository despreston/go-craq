// transport package are interfaces for the transport layer of the application.
// The purpose is to make it easy to swap one type of transport (e.g. net/rpc)
// for another (e.g. gRPC). The preferred mode of communication is RPC and so
// communication follows the remote-procedure paradigm, but nothing's stopping
// anyone from writing whatever implementation they prefer.

package transport

// Transporter is the vehicle for inter-chain communication.
type Transporter interface {
	Connect(string) (Client, error)
}

// Client facilitates communication.
type Client interface {
	// Call is for invoking remote procedure calls.
	Call(method string, args interface{}, reply interface{}) error

	// Close the connection.
	Close() error
}
