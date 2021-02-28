package netrpc

import "github.com/despreston/go-craq/transport"

func NewNodeClient() transport.NodeClient {
	return &NodeClient{Client: &Client{}}
}

type NodeClient struct {
	*Client
}

func (nc *NodeClient) Ping() error {
	return nc.Client.rpc.Call(
		"RPC.Ping",
		&EmptyArgs{},
		&EmptyReply{},
	)
}

func (nc *NodeClient) Update(meta *transport.NodeMeta) error {
	return nc.Client.rpc.Call(
		"RPC.Update",
		meta,
		&EmptyReply{},
	)
}

func (nc *NodeClient) LatestVersion(key string) (string, uint64, error) {
	reply := VersionResponse{}
	err := nc.Client.rpc.Call("RPC.Commit", key, &reply)
	return reply.Key, reply.Version, err
}

func (nc *NodeClient) Commit(key string, version uint64) error {
	return nc.Client.rpc.Call(
		"RPC.Commit",
		&CommitArgs{Key: key, Version: version},
		&EmptyReply{},
	)
}

func (nc *NodeClient) Read(key string) (string, []byte, error) {
	reply := &transport.Item{}
	err := nc.Client.rpc.Call("RPC.Read", key, reply)
	return reply.Key, reply.Value, err
}

func (nc *NodeClient) Write(key string, value []byte, version uint64) error {
	return nc.Client.rpc.Call(
		"RPC.Write",
		&WriteArgs{Key: key, Value: value, Version: version},
		&EmptyReply{},
	)
}

func (nc *NodeClient) ClientWrite(key string, value []byte) error {
	return nc.Client.rpc.Call(
		"RPC.ClientWrite",
		&ClientWriteArgs{Key: key, Value: value},
		&EmptyReply{},
	)
}

func (nc *NodeClient) BackPropagate(
	vByK *transport.PropagateRequest,
) (*transport.PropagateResponse, error) {
	reply := &transport.PropagateResponse{}
	if err := nc.Client.rpc.Call("RPC.BackPropagate", vByK, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (nc *NodeClient) FwdPropagate(
	vByK *transport.PropagateRequest,
) (*transport.PropagateResponse, error) {
	reply := &transport.PropagateResponse{}
	if err := nc.Client.rpc.Call("RPC.FwdPropagate", vByK, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (nc *NodeClient) ReadAll() (*[]transport.Item, error) {
	reply := &[]transport.Item{}
	if err := nc.Client.rpc.Call("RPC.ReadAll", &EmptyArgs{}, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

// NodeBinding provides a layer of translation between the
// NodeService which is transport agnostic and the net/rpc package. This
// allows using the net/rpc package to invoke NodeService methods.
type NodeBinding struct {
	Svc transport.NodeService
}

func (n *NodeBinding) Ping(_ *EmptyArgs, _ *EmptyReply) error {
	return n.Svc.Ping()
}

func (n *NodeBinding) Update(args *transport.NodeMeta, _ *EmptyReply) error {
	return n.Svc.Update(args)
}

func (n *NodeBinding) ClientWrite(args *ClientWriteArgs, _ *EmptyReply) error {
	return n.Svc.ClientWrite(args.Key, args.Value)
}

func (n *NodeBinding) Write(args *WriteArgs, _ *EmptyReply) error {
	return n.Svc.Write(args.Key, args.Value, args.Version)
}

func (n *NodeBinding) LatestVersion(key string, reply *VersionResponse) error {
	key, version, err := n.Svc.LatestVersion(key)
	if err != nil {
		return err
	}
	reply.Key = key
	reply.Version = version
	return nil
}

func (n *NodeBinding) FwdPropagate(
	args *transport.PropagateRequest,
	reply *transport.PropagateResponse,
) error {
	r, err := n.Svc.FwdPropagate(args)
	*reply = *r
	return err
}

func (n *NodeBinding) BackPropagate(
	args *transport.PropagateRequest,
	reply *transport.PropagateResponse,
) error {
	r, err := n.Svc.BackPropagate(args)
	*reply = *r
	return err
}

func (n *NodeBinding) Commit(args *CommitArgs, _ *EmptyReply) error {
	return n.Svc.Commit(args.Key, args.Version)
}

func (n *NodeBinding) Read(key string, reply *transport.Item) error {
	key, value, err := n.Svc.Read(key)
	if err != nil {
		return err
	}
	reply.Key = key
	reply.Value = value
	return nil
}

func (n *NodeBinding) ReadAll(_ *EmptyArgs, reply *[]transport.Item) error {
	items, err := n.Svc.ReadAll()
	if err != nil {
		return err
	}
	*reply = *items
	return nil
}
