/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package agentserver

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"google.golang.org/grpc/metadata"
	"k8s.io/klog"
	"sigs.k8s.io/apiserver-network-proxy/pkg/agent/common"
	"sigs.k8s.io/apiserver-network-proxy/proto/agent"
)

// ProxyClientConnection...
type ProxyClientConnection struct {
	Mode      string
	Grpc      agent.ProxyService_ProxyServer
	HTTP      net.Conn
	connected chan struct{}
	connectID int64
}

func (c *ProxyClientConnection) send(pkt *agent.Packet) error {
	if c.Mode == "grpc" {
		stream := c.Grpc
		return stream.Send(pkt)
	} else if c.Mode == "http-connect" {
		if pkt.Type == agent.PacketType_CLOSE_RSP {
			return c.HTTP.Close()
		} else if pkt.Type == agent.PacketType_DATA {
			_, err := c.HTTP.Write(pkt.GetData().Data)
			return err
		} else if pkt.Type == agent.PacketType_DIAL_RSP {
			return nil
		} else {
			return fmt.Errorf("attempt to send via unrecognized connection type %v", pkt.Type)
		}
	} else {
		return fmt.Errorf("attempt to send via unrecognized connection mode %q", c.Mode)
	}
}

// ProxyServer
type ProxyServer struct {
	// backends saves the mapping from agentID to gRPC stream
	backends map[string]agent.AgentService_ConnectServer

	lock sync.Mutex

	// connID track
	connAgent   map[int64]string
	Frontends   map[int64]*ProxyClientConnection
	PendingDial map[int64]*ProxyClientConnection
	frontLock   sync.Mutex
}

var _ agent.AgentServiceServer = &ProxyServer{}

var _ agent.ProxyServiceServer = &ProxyServer{}

// NewProxyServer creates a new ProxyServer instance
func NewProxyServer() *ProxyServer {
	return &ProxyServer{
		Frontends:   make(map[int64]*ProxyClientConnection),
		PendingDial: make(map[int64]*ProxyClientConnection),
	}
}

// Proxy handles incoming streams from gRPC frontend.
func (s *ProxyServer) Proxy(stream agent.ProxyService_ProxyServer) error {
	klog.Info("proxy request from client")

	recvCh := make(chan *agent.Packet, 10)
	stopCh := make(chan error)

	go s.serveRecvFrontend(stream, recvCh)

	defer func() {
		close(recvCh)
	}()

	// Start goroutine to receive packets from frontend and push to recvCh
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				close(stopCh)
				return
			}
			if err != nil {
				klog.Warningf(">>> Stream read from frontend error: %v", err)
				close(stopCh)
				return
			}

			recvCh <- in
		}
	}()

	return <-stopCh
}

func (s *ProxyServer) serveRecvFrontend(stream agent.ProxyService_ProxyServer, recvCh <-chan *agent.Packet) {
	klog.Info("start serving frontend stream")

	var firstConnID int64

	for pkt := range recvCh {
		switch pkt.Type {
		case agent.PacketType_DIAL_REQ:
			klog.Info(">>> Received DIAL_REQ")

			backend := s.getRandomBackend()

			if backend == nil {
				klog.Info(">>> No backend found; drop")
				continue
			}

			if err := backend.Send(pkt); err != nil {
				klog.Warningf(">>> DIAL_REQ to backend failed: %v", err)
			}
			s.frontLock.Lock()
			s.PendingDial[pkt.GetDialRequest().Random] = &ProxyClientConnection{
				Mode:      "grpc",
				Grpc:      stream,
				connected: make(chan struct{}),
			}
			s.frontLock.Unlock()
			klog.Info(">>> DIAL_REQ sent to backend") // got this. but backend didn't receive anything.

		case agent.PacketType_CLOSE_REQ:
			connid := pkt.GetCloseRequest().ConnectID
			klog.Infof(">>> Received CLOSE_REQ(id=%d)", connid)

			// Routed to the original backend
			agentID := s.getAgent(connid)
			if agentID == "" {
				klog.Warningf("CLOSE_REQ(id=%d) dropped: no valid agent found"), connid)
				continue
			}

			backend := s.getBackend(agentID)
			if backend == nil {
				klog.Warningf("CLOSE_REQ(id=%d) dropped: no valid backend found"), connid)
				continue
			}

			if backend == nil {
				klog.Info(">>> No backend found; drop")
				continue
			}

			if err := backend.Send(pkt); err != nil {
				klog.Warningf(">>> CLOSE_REQ to backend failed: %v", err)
			}
			klog.Info("CLOSE_REQ sent to backend")

		case agent.PacketType_DATA:
			connID := pkt.GetData().ConnectID
			klog.Infof(">>> Received DATA(id=%d)", connID)

			if firstConnID == 0 {
				firstConnID = connID
			} else if firstConnID != connID {
				klog.Warningf(">>> Data(id=%d) doesn't match first connection id %d", firstConnID, connID)
			}

			backend := s.getBackend()
			if backend == nil {
				klog.Info(">>> No backend found; drop")
				continue
			}

			if err := backend.Send(pkt); err != nil {
				klog.Warningf(">>> DATA to backend failed: %v", err)
			}
			klog.Info(">>> DATA sent to backend")

		default:
			klog.Infof(">>> Ignore %v packet coming from frontend", pkt.Type)
		}
	}

	klog.Infof(">>> Close streaming (id=%d)", firstConnID)

	pkt := &agent.Packet{
		Type: agent.PacketType_CLOSE_REQ,
		Payload: &agent.Packet_CloseRequest{
			CloseRequest: &agent.CloseRequest{
				ConnectID: firstConnID,
			},
		},
	}

	backend := s.getBackend()
	if backend != nil {
		if err := backend.Send(pkt); err != nil {
			klog.Warningf(">>> CLOSE_REQ to backend failed: %v", err)
		}
	}
}

func (s *ProxyServer) serveSend(stream agent.ProxyService_ProxyServer, sendCh <-chan *agent.Packet) {
	klog.Info("start serve send ...")
	for pkt := range sendCh {
		err := stream.Send(pkt)
		if err != nil {
			klog.Warningf("stream write error: %v", err)
		}
	}
}

// Connect is for agent to connect to ProxyServer as next hop
func (s *ProxyServer) Connect(stream agent.AgentService_ConnectServer) error {	
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		klog.Info("Missing metadata, dropping connect")
		return errors.New("Missing metadata")
	}
	
	agentID := md[common.AgentIDKey]
	if agentID == "" {
		klog.Info("Missing agent-id in metadata, dropping connect")
		return errors.New("Missing agent-id in metadata")
	}

	klog.Info("connect request from agent %s", agentID)
	s.setBackend(agentID, stream)

	defer func() {
		klog.Infof("unregister Backend %v", stream)
		s.deleteBackend(agentID)
	}()

	recvCh := make(chan *agent.Packet, 10)
	stopCh := make(chan error)

	go s.serveRecvBackend(stream, recvCh)

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
				return
			}

			recvCh <- in
		}
	}()

	return <-stopCh
}

// route the packet back to the correct client
func (s *ProxyServer) serveRecvBackend(stream agent.AgentService_ConnectServer, recvCh <-chan *agent.Packet) {
	var firstConnID int64
	var client *ProxyClientConnection
	var ok bool

	for pkt := range recvCh {
		switch pkt.Type {
		case agent.PacketType_DIAL_RSP:
			resp := pkt.GetDialResponse()
			firstConnID = resp.ConnectID
			klog.Infof("<<< Received DIAL_RSP(rand=%d, id=%d)", resp.Random, resp.ConnectID)

			s.frontLock.Lock()
			client, ok = s.PendingDial[resp.Random]
			s.frontLock.Unlock()

			if !ok {
				klog.Warning("<<< DialResp not recognized; dropped")
				continue
			} else {
				s.frontLock.Lock()
				delete(s.PendingDial, resp.Random)
				s.frontLock.Unlock()
			}

			err := client.send(pkt)
			if err != nil {
				klog.Warningf("<<< DIAL_RSP send to client stream error: %v", err)
			} else {
				client.connectID = resp.ConnectID
				s.frontLock.Lock()
				s.Frontends[resp.ConnectID] = client
				s.frontLock.Unlock()
				close(client.connected)
			}

		case agent.PacketType_DATA:
			resp := pkt.GetData()
			klog.Infof("<<< Received DATA(id=%d)", resp.ConnectID)
			s.frontLock.Lock()
			client, ok = s.Frontends[resp.ConnectID]
			s.frontLock.Unlock()
			if ok {
				if err := client.send(pkt); err != nil {
					klog.Warningf("<<< DATA send to client stream error: %v", err)
				} else {
					klog.Infof("<<< DATA sent to frontend")
				}
			}

		case agent.PacketType_CLOSE_RSP:
			resp := pkt.GetCloseResponse()
			klog.Infof("<<< Received CLOSE_RSP(id=%d)", resp.ConnectID)
			s.frontLock.Lock()
			client, ok = s.Frontends[resp.ConnectID]
			s.frontLock.Unlock()

			if ok {
				if err := client.send(pkt); err != nil {
					// Normal when frontend closes it.
					klog.Warningf("<<< CLOSE_RSP send to client stream error: %v", err)
				} else {
					klog.Infof("<<< CLOSE_RSP sent to frontend")
				}
			}

		default:
			klog.Warningf("<<< Unrecognized packet %+v", pkt)
		}
	}

	klog.Infof("<<< Close streaming (id=%d)", firstConnID)
}

func (s *ProxyServer) getBackend(agentID string) agent.AgentService_ConnectServer {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.backends[agentID]
}

func (s *ProxyServer) getRandomBackend() agent.AgentService_ConnectServer {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, v := range s.backends {
		return v
	}
	return nil
}

func (s *ProxyServer) setBackend(agentID string, backend agent.AgentService_ConnectServer) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.backends[agentID] = backend
}

func (s *ProxyServer) deleteBackend(agentID string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.backends, agentID)
}

func (s *ProxyServer) getAgent(connID int64) string {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.connAgent[connID]
}