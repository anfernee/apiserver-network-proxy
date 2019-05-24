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

package agentclient

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"k8s.io/klog"
	"sigs.k8s.io/apiserver-network-proxy/proto/agent"
)

type AgentClient struct {
	nextConnID int64
	conns      map[int64]net.Conn
	dataChs    map[int64]chan []byte
	cleanups   map[int64]func()
	address    string

	stream agent.AgentService_ConnectClient
}

func NewAgentClient(address string) *AgentClient {
	a := &AgentClient{
		conns:    make(map[int64]net.Conn),
		address:  address,
		dataChs:  make(map[int64]chan []byte),
		cleanups: make(map[int64]func()),
	}

	return a
}

func (a *AgentClient) Connect(opts ...grpc.DialOption) error {
	c, err := grpc.Dial(a.address, opts...)
	if err != nil {
		return err
	}

	client := agent.NewAgentServiceClient(c)

	a.stream, err = client.Connect(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func (a *AgentClient) Serve(stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			klog.Info("stop agent client.")
			return
		default:
		}

		klog.Info("waiting packets...")

		pkt, err := a.stream.Recv()
		if err == io.EOF {
			klog.Info("received EOF, exit")
			return
		}
		if err != nil {
			klog.Warningf("stream read error: %v", err)
			return
		}

		klog.Infof("[tracing] recv packet %+v", pkt)

		switch pkt.Type {
		case agent.PacketType_DIAL_REQ:
			klog.Info("received DIAL_REQ")
			resp := &agent.Packet{
				Type:    agent.PacketType_DIAL_RSP,
				Payload: &agent.Packet_DialResponse{DialResponse: &agent.DialResponse{}},
			}

			dialReq := pkt.GetDialRequest()
			resp.GetDialResponse().Random = dialReq.Random

			conn, err := net.Dial(dialReq.Protocol, dialReq.Address)
			if err != nil {
				resp.GetDialResponse().Error = err.Error()
				if err := a.stream.Send(resp); err != nil {
					klog.Warningf("stream send error: %v", err)
				}
				continue
			}

			connID := atomic.AddInt64(&a.nextConnID, 1)
			a.conns[connID] = conn
			a.dataChs[connID] = make(chan []byte, 5)
			var once sync.Once

			cleanup := func() {
				once.Do(func() {
					resp := &agent.Packet{
						Type:    agent.PacketType_CLOSE_RSP,
						Payload: &agent.Packet_CloseResponse{CloseResponse: &agent.CloseResponse{}},
					}
					resp.GetCloseResponse().ConnectID = connID

					err := conn.Close()
					if err != nil {
						resp.GetCloseResponse().Error = err.Error()
					}

					if err := a.stream.Send(resp); err != nil {
						klog.Warningf("stream send error: %v", err)
					}

					close(a.dataChs[connID])
					delete(a.conns, connID)
					delete(a.dataChs, connID)
					delete(a.cleanups, connID)
				})
			}

			a.cleanups[connID] = cleanup

			resp.GetDialResponse().ConnectID = connID
			if err := a.stream.Send(resp); err != nil {
				klog.Warningf("stream send error: %v", err)
				continue
			}

			go a.remoteToProxy(conn, connID, cleanup)
			go a.proxyToRemote(conn, a.dataChs[connID], cleanup)

		case agent.PacketType_DATA:
			klog.Info("received DATA")
			data := pkt.GetData()
			klog.Infof("[tracing] %v", data)

			if dataCh, ok := a.dataChs[data.ConnectID]; ok {
				dataCh <- data.Data
			}

		case agent.PacketType_CLOSE_REQ:
			klog.Info("received CLOSE_REQ")

			closeReq := pkt.GetCloseRequest()
			connID := closeReq.ConnectID

			cleanup := a.cleanups[connID]
			if cleanup == nil {
				resp := &agent.Packet{
					Type:    agent.PacketType_CLOSE_RSP,
					Payload: &agent.Packet_CloseResponse{CloseResponse: &agent.CloseResponse{}},
				}
				resp.GetCloseResponse().ConnectID = connID
				resp.GetCloseResponse().Error = "Unknown connectID"
				if err := a.stream.Send(resp); err != nil {
					klog.Warningf("stream send error: %v", err)
					continue
				}
			} else {
				cleanup()
			}

		default:
			klog.Warningf("unrecognized packet type: %+v", pkt)
		}
	}
}

func (a *AgentClient) remoteToProxy(conn net.Conn, connID int64, cleanup func()) {
	defer cleanup()

	var buf [1 << 12]byte
	resp := &agent.Packet{
		Type: agent.PacketType_DATA,
	}

	for {
		n, err := conn.Read(buf[:])

		if err == io.EOF {
			klog.Info("connection EOF")
			return
		} else if err != nil {
			klog.Warningf("connection read error: %v", err)
			return
		} else {
			resp.Payload = &agent.Packet_Data{Data: &agent.Data{
				Data:      buf[:n],
				ConnectID: connID,
			}}
			if err := a.stream.Send(resp); err != nil {
				klog.Warningf("stream send error: %v", err)
			}
		}
	}
}

func (a *AgentClient) proxyToRemote(conn net.Conn, dataCh <-chan []byte, cleanup func()) {
	defer cleanup()

	for d := range dataCh {
		pos := 0
		for {
			n, err := conn.Write(d[pos:])
			if err == nil {
				break
			} else if n > 0 {
				pos += n
			} else {
				klog.Errorf("conn write error: %v", err)
				return
			}
		}
	}
}
