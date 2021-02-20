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
		&transport.EmptyArgs{},
		&transport.EmptyReply{},
	)
}

func (nc *NodeClient) Update(meta *transport.NodeMeta) error {
	return nc.Client.rpc.Call(
		"RPC.Update",
		meta,
		&transport.EmptyReply{},
	)
}

func (nc *NodeClient) LatestVersion(key string) (string, uint64, error) {
	reply := transport.VersionResponse{}
	err := nc.Client.rpc.Call("RPC.Commit", key, &reply)
	return reply.Key, reply.Version, err
}

func (nc *NodeClient) Commit(key string, version uint64) error {
	return nc.Client.rpc.Call(
		"RPC.Commit",
		&transport.CommitArgs{Key: key, Version: version},
		&transport.EmptyReply{},
	)
}

func (nc *NodeClient) Read(key string) (string, []byte, error) {
	reply := &transport.ReadResponse{}
	err := nc.Client.rpc.Call("RPC.Read", key, reply)
	return reply.Key, reply.Value, err
}

func (nc *NodeClient) Write(key string, value []byte, version uint64) error {
	return nc.Client.rpc.Call(
		"RPC.Write",
		&transport.WriteArgs{Key: key, Value: value, Version: version},
		&transport.EmptyReply{},
	)
}

func (nc *NodeClient) ClientWrite(key string, value []byte) error {
	return nc.Client.rpc.Call(
		"RPC.ClientWrite",
		&transport.ClientWriteArgs{Key: key, Value: value},
		&transport.EmptyReply{},
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

// NodeBinding provides a layer of translation between the
// NodeService which is transport agnostic and the net/rpc package. This
// allows using the net/rpc package to invoke NodeService methods.
type NodeBinding struct {
	Svc transport.NodeService
}

func (n *NodeBinding) Ping(_ *transport.EmptyArgs, _ *transport.EmptyReply) error {
	return n.Svc.Ping()
}

func (n *NodeBinding) Update(args *transport.NodeMeta, _ *transport.EmptyReply) error {
	return n.Svc.Update(args)
}

func (n *NodeBinding) ClientWrite(args *transport.ClientWriteArgs, _ *transport.EmptyReply) error {
	return n.Svc.ClientWrite(args.Key, args.Value)
}

func (n *NodeBinding) Write(args *transport.WriteArgs, _ *transport.EmptyReply) error {
	return n.Svc.Write(args.Key, args.Value, args.Version)
}

func (n *NodeBinding) LatestVersion(key string, reply *transport.VersionResponse) error {
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
	reply = r
	return err
}

func (n *NodeBinding) BackPropagate(
	args *transport.PropagateRequest,
	reply *transport.PropagateResponse,
) error {
	r, err := n.Svc.BackPropagate(args)
	reply = r
	return err
}

func (n *NodeBinding) Commit(args *transport.CommitArgs, _ *transport.EmptyReply) error {
	return n.Svc.Commit(args.Key, args.Version)
}

func (n *NodeBinding) Read(key string, reply *transport.ReadResponse) error {
	key, value, err := n.Svc.Read(key)
	if err != nil {
		return err
	}
	reply.Key = key
	reply.Value = value
	return nil
}
