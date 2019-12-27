package agentserver

import (
	"errors"
	"fmt"
	"sync"

	"sigs.k8s.io/apiserver-network-proxy/proto/agent"
)

// state is used by both agent and proxy server to track connection state
type state struct {
	// connection-id to client connection mapping
	frontends map[int64]*ProxyClientConnection

	// dial-req-random to client connection mapping
	pendingDial map[int64]*ProxyClientConnection

	sync.Mutex
}

func newState() *state {
	return &state{
		frontends:   make(map[int64]*ProxyClientConnection),
		pendingDial: make(map[int64]*ProxyClientConnection),
	}
}

func (s *state) NewConnection(random int64, conn *ProxyClientConnection) error {
	s.Lock()
	defer s.Unlock()

	s.pendingDial[random] = conn
	// TODO(anfernee): auto cleanup pending dial
	return nil
}

func (s *state) ConnectionEstablished(pkt *agent.Packet) error {
	s.Lock()
	defer s.Unlock()

	resp := pkt.GetDialResponse()
	if resp == nil {
		// wrong packet type, no-op
		return nil
	}

	random, connid := resp.Random, resp.ConnectID

	conn, ok := s.pendingDial[random]
	if !ok {
		return fmt.Errorf("DialResp[random=%d] not recognized", random)
	}

	delete(s.pendingDial, random)

	if err := conn.send(pkt); err != nil {
		// TODO(anfernee): conn.Close()?
		return errors.New("Failed to forward dial response to client")
	}

	close(conn.connected)
	s.frontends[connid] = conn
	return nil
}

func (s *state) GetConnection(connid int64) (*ProxyClientConnection, error) {
	s.Lock()
	defer s.Unlock()

	conn, ok := s.frontends[connid]
	if !ok {
		return nil, errors.New("connection not recognized")
	}

	return conn, nil
}
