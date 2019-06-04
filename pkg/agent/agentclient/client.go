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
	nextConnID  int64
	connContext map[int64]*connContext
	address     string

	stream agent.AgentService_ConnectClient
}

type connContext struct {
	conn      net.Conn
	cleanFunc func()
	dataCh    chan []byte
	cleanOnce sync.Once
}

func (c *connContext) cleanup() {
	c.cleanOnce.Do(c.cleanFunc)
}

func NewAgentClient(address string) *AgentClient {
	a := &AgentClient{
		connContext: make(map[int64]*connContext),
		address:     address,
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
			dataCh := make(chan []byte, 5)
			a.connContext[connID] = &connContext{
				conn:   conn,
				dataCh: dataCh,
				cleanFunc: func() {
					klog.Infof("close connection(id=%d)", connID)
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
						klog.Warningf("close response send error: %v", err)
					}

					close(dataCh)
					delete(a.connContext, connID)
				},
			}

			resp.GetDialResponse().ConnectID = connID
			if err := a.stream.Send(resp); err != nil {
				klog.Warningf("stream send error: %v", err)
				continue
			}

			go a.remoteToProxy(conn, connID)
			go a.proxyToRemote(conn, connID)

		case agent.PacketType_DATA:
			data := pkt.GetData()
			klog.Infof("received DATA(id=%d)", data.ConnectID)
			klog.Infof("[tracing] %v", data)

			if ctx, ok := a.connContext[data.ConnectID]; ok {
				ctx.dataCh <- data.Data
			}

		case agent.PacketType_CLOSE_REQ:
			closeReq := pkt.GetCloseRequest()
			connID := closeReq.ConnectID

			klog.Infof("received CLOSE_REQ(id=%d)", connID)

			if ctx, ok := a.connContext[connID]; ok {
				ctx.cleanup()
			} else {
				resp := &agent.Packet{
					Type:    agent.PacketType_CLOSE_RSP,
					Payload: &agent.Packet_CloseResponse{CloseResponse: &agent.CloseResponse{}},
				}
				resp.GetCloseResponse().ConnectID = connID
				resp.GetCloseResponse().Error = "Unknown connectID"
				if err := a.stream.Send(resp); err != nil {
					klog.Warningf("close response send error: %v", err)
					continue
				}
			}

		default:
			klog.Warningf("unrecognized packet type: %+v", pkt)
		}
	}
}

func (a *AgentClient) remoteToProxy(conn net.Conn, connID int64) {
	ctx := a.connContext[connID]
	if ctx == nil {
		return
	}

	defer ctx.cleanup()

	var buf [1 << 12]byte
	resp := &agent.Packet{
		Type: agent.PacketType_DATA,
	}

	for {
		n, err := conn.Read(buf[:])
		klog.Infof("received %d bytes from proxy server", n)

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

func (a *AgentClient) proxyToRemote(conn net.Conn, connID int64) {
	ctx := a.connContext[connID]
	if ctx == nil {
		return
	}

	defer ctx.cleanup()

	for d := range ctx.dataCh {
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
