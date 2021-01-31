package transport

type Transporter interface {
	Connect(string) (Client, error)
}

type Client interface {
	Call(string, interface{}, interface{}) error
}
