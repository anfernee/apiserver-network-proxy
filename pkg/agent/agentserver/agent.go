package agentserver

import (
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"sync"

	"google.golang.org/grpc/metadata"
	"k8s.io/klog"
	"sigs.k8s.io/apiserver-network-proxy/proto/agent"
)

type agentServer struct {
	mu sync.Mutex

	// A map between agentID and its grpc connections.
	// For a given agent, ProxyServer prefers backends[agentID][0] to send
	// traffic, because backends[agentID][1:] are more likely to be closed
	// by the agent to deduplicate connections to the same server.
	backends map[string][]agent.AgentService_ConnectServer
	agentIDs []string

	state *state

	// ServerID is the ID that client has to provide to be able to connect to server.
	// TODO(anfernee): Link to doc
	ServerID string

	// ServerCount is the number of servers the client has to connect when server is
	// behind a Load Balancer
	// TODO(anfernee): Link to doc
	ServerCount int
}

func newAgentServer(serverID string, serverCount int, state *state) *agentServer {
	return &agentServer{
		backends:    make(map[string][]agent.AgentService_ConnectServer),
		ServerID:    serverID,
		ServerCount: serverCount,
		state:       state,
	}
}

var _ agent.AgentServiceServer = &agentServer{}

// Connect is for agent to connect to ProxyServer as next hop
func (s *agentServer) Connect(stream agent.AgentService_ConnectServer) error {
	klog.Info("connect request from Backend")
	agentID, err := agentID(stream)
	if err != nil {
		return err
	}
	s.addBackend(agentID, stream)
	defer s.removeBackend(agentID, stream)

	header := metadata.Pairs("serverID", s.ServerID, "serverCount", strconv.Itoa(s.ServerCount))
	if err := stream.SendHeader(header); err != nil {
		return err
	}

	recvCh := make(chan *agent.Packet, 10)
	stopCh := make(chan error)

	go serveRecvBackend(stream, recvCh, s.state)

	defer func() {
		close(recvCh)
	}()

	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				close(stopCh)
				return
			}
			if err != nil {
				klog.Warningf("stream read error: %v", err)
				close(stopCh)
				return
			}

			recvCh <- in
		}
	}()

	return <-stopCh
}

/*
func (s *agentServer) Route(context RouteContext) (Backend, error) {
	b, err := s.randomBackend()
	if err != nil {
		return nil, err
	}

	return Backend(b), nil
}
*/

func (s *agentServer) addBackend(agentID string, conn agent.AgentService_ConnectServer) {
	klog.Infof("register Backend %v for agentID %s", conn, agentID)
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.backends {
		if k == agentID {
			s.backends[k] = append(s.backends[k], conn)
			return
		}
	}
	s.backends[agentID] = []agent.AgentService_ConnectServer{conn}
	s.agentIDs = append(s.agentIDs, agentID)
}

func (s *agentServer) removeBackend(agentID string, conn agent.AgentService_ConnectServer) {
	klog.Infof("remove Backend %v for agentID %s", conn, agentID)
	s.mu.Lock()
	defer s.mu.Unlock()
	backends, ok := s.backends[agentID]
	if !ok {
		klog.Warningf("can't find agentID %s in the backends", agentID)
		return
	}
	var found bool
	for i, c := range backends {
		if c == conn {
			s.backends[agentID] = append(s.backends[agentID][:i], s.backends[agentID][i+1:]...)
			found = true
		}
	}
	if len(s.backends[agentID]) == 0 {
		delete(s.backends, agentID)
		for i := range s.agentIDs {
			if s.agentIDs[i] == agentID {
				s.agentIDs[i] = s.agentIDs[len(s.agentIDs)-1]
				s.agentIDs = s.agentIDs[:len(s.agentIDs)-1]
				break
			}
		}
	}
	if !found {
		klog.Warningf("can't find conn %v for agentID %s in the backends", conn, agentID)
	}
}

func (s *agentServer) randomBackend() (agent.AgentService_ConnectServer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.backends) == 0 {
		return nil, fmt.Errorf("No backend available")
	}
	agentID := s.agentIDs[rand.Intn(len(s.agentIDs))]
	return s.backends[agentID][0], nil
}

func agentID(stream agent.AgentService_ConnectServer) (string, error) {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return "", fmt.Errorf("failed to get context")
	}
	agentIDs := md.Get("agentID")
	if len(agentIDs) != 1 {
		return "", fmt.Errorf("expected one agent ID in the context, got %v", agentIDs)
	}
	return agentIDs[0], nil
}

func serveRecvBackend(stream agent.AgentService_ConnectServer, recvCh <-chan *agent.Packet, state *state) {
	var firstConnID int64

	for pkt := range recvCh {
		switch pkt.Type {
		case agent.PacketType_DIAL_RSP:
			resp := pkt.GetDialResponse()
			firstConnID = resp.ConnectID
			klog.V(5).Infof("<<< Received DIAL_RSP(rand=%d, id=%d)", resp.Random, resp.ConnectID)

			if err := state.ConnectionEstablished(pkt); err != nil {
				klog.Warningf("Failed to establish connection: %v", err)
			}

		case agent.PacketType_DATA:
			resp := pkt.GetData()
			klog.V(5).Infof("<<< Received DATA(id=%d)", resp.ConnectID)

			if client, err := state.GetConnection(resp.ConnectID); err != nil {
				klog.Warning(err)
			} else {
				if err := client.send(pkt); err != nil {
					klog.Warningf("<<< DATA send to client stream error: %v", err)
				} else {
					klog.V(5).Infof("<<< DATA sent to frontend")
				}
			}

		case agent.PacketType_CLOSE_RSP:
			resp := pkt.GetCloseResponse()
			klog.V(5).Infof("<<< Received CLOSE_RSP(id=%d)", resp.ConnectID)

			if client, err := state.GetConnection(resp.ConnectID); err != nil {
				klog.Warning(err)
			} else {
				if err := client.send(pkt); err != nil {
					klog.Warningf("<<< CLOSE_RSP send to client stream error: %v", err)
				} else {
					klog.V(5).Infof("<<< CLOSE_RSP sent to frontend")
				}
			}

		default:
			klog.Warningf("<<< Unrecognized packet %+v", pkt)
		}
	}

	klog.V(4).Infof("<<< Close streaming (id=%d)", firstConnID)
}
