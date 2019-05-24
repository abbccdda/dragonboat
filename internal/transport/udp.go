// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"context"
	"fmt"

	//"context"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/raftio"
	"github.com/lni/dragonboat/raftpb"
	"net"
	"sync"
	"time"
)

const (
	// UDPRaftRPCName is the name of the tcp RPC module.
	UDPRaftRPCName           = "go-tcp-transport"
)

// UDPTransport is a UDP based RPC module for exchanging raft messages and
// snapshots between NodeHost instances.
type UDPTransport struct {
	nhConfig       config.NodeHostConfig
	stopper        *syncutil.Stopper
	requestHandler raftio.RequestHandler
	sinkFactory    raftio.ChunkSinkFactory
}

// UDPConnection is the connection used for sending raft messages to remote
// nodes.
type UDPConnection struct {
	conn    net.Conn
	header  []byte
	payload []byte
	target  string
}

// NewUDPConnection creates and returns a new TCPConnection instance.
func NewUDPConnection(conn net.Conn) *TCPConnection {
	return &TCPConnection{
		conn:    conn,
		header:  make([]byte, requestHeaderSize),
		payload: make([]byte, perConnBufSize),
	}
}


// UDPSnapshotConnection is the connection for sending raft snapshot chunks to
// remote nodes.
type UDPSnapshotConnection struct {
	conn   net.Conn
	header []byte
}

// NewUDPSnapshotConnection creates and returns a new snapshot connection.
func NewUDPSnapshotConnection(conn net.Conn) *UDPSnapshotConnection {
	return &UDPSnapshotConnection{
		conn:   conn,
		header: make([]byte, requestHeaderSize),
	}
}

// Close closes the snapshot connection.
func (c *UDPSnapshotConnection) Close() {
	if err := c.conn.Close(); err != nil {
		plog.Errorf("failed to close the snapshot connection %v", err)
	}
}

// Start starts the UDP transport module.
func (g *UDPTransport) Start() error {
	address := g.nhConfig.GetListenAddress()

	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", addr)

	if err != nil {
		plog.Panicf("failed to new a stoppable listener, %v", err)
	}
	g.stopper.RunWorker(func() {
		for {
			p := make([]byte, 2048)

			_, remoteaddr, err := conn.ReadFromUDP(p)
			fmt.Printf("Read a message from %v %s \n", remoteaddr, p)
			if err != nil {
				fmt.Printf("Some error  %v", err)
				continue
			}

			_, err = conn.WriteToUDP([]byte("From server: Hello I got your message "), remoteaddr)
			if err != nil {
				fmt.Printf("Couldn't send response %v", err)
			}

			var once sync.Once
			closeFn := func() {
				once.Do(func() {
					if err := conn.Close(); err != nil {
						plog.Errorf("failed to close the connection %v", err)
					}
				})
			}
			g.stopper.RunWorker(func() {
				<-g.stopper.ShouldStop()
				closeFn()
			})
			g.stopper.RunWorker(func() {
				g.serveConn(conn)
				closeFn()
			})
		}
	})
	return nil
}

func (g *UDPTransport) serveConn(conn net.Conn) {
	magicNum := make([]byte, len(magicNumber))
	header := make([]byte, requestHeaderSize)
	tbuf := make([]byte, payloadBufferSize)
	var chunks raftio.IChunkSink
	stopper := syncutil.NewStopper()
	defer func() {
		if chunks != nil {
			chunks.Close()
		}
	}()
	defer stopper.Stop()
	for {
		err := readMagicNumber(conn, magicNum)
		if err != nil {
			if err == ErrBadMessage {
				return
			}
			operr, ok := err.(net.Error)
			if ok && operr.Timeout() {
				continue
			} else {
				return
			}
		}
		rheader, buf, err := readMessage(conn, header, tbuf)
		if err != nil {
			return
		}
		if rheader.method == raftType {
			batch := raftpb.MessageBatch{}
			if err := batch.Unmarshal(buf); err != nil {
				return
			}
			g.requestHandler(batch)
		} else {
			chunk := raftpb.SnapshotChunk{}
			if err := chunk.Unmarshal(buf); err != nil {
				return
			}
			if chunks == nil {
				chunks = g.sinkFactory()
				stopper.RunWorker(func() {
					ticker := time.NewTicker(time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							chunks.Tick()
						case <-stopper.ShouldStop():
							return
						}
					}
				})
			}
			chunks.AddChunk(chunk)
		}
	}
}
// NewUDPTransport creates and returns a new UDP transport module.

func NewUDPTransport(nhConfig config.NodeHostConfig,
	requestHandler raftio.RequestHandler,
	sinkFactory raftio.ChunkSinkFactory) raftio.IRaftRPC {
	plog.Infof("Using the default UDP RPC, switch to gRPC based RPC module " +
		"(github.com/lni/dragonboat/plugin/rpc) for HTTP2 based transport")
	return &UDPTransport{
		nhConfig:       nhConfig,
		stopper:        syncutil.NewStopper(),
		requestHandler: requestHandler,
		sinkFactory:    sinkFactory,
	}
}


// Stop stops the UDP transport module.
func (g *UDPTransport) Stop() {
	g.stopper.Stop()
}

// GetConnection returns a new raftio.IConnection for sending raft messages.
func (g *UDPTransport) GetConnection(ctx context.Context,
	target string) (raftio.IConnection, error) {
	conn, err := g.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewUDPConnection(conn), nil
}


// Close closes the UDPConnection instance.
func (c *UDPConnection) Close() {
	if err := c.conn.Close(); err != nil {
		plog.Errorf("failed to close the connection %v", err)
	}
}

// SendMessageBatch sends a raft message batch to remote node.
func (c *UDPConnection) SendMessageBatch(batch raftpb.MessageBatch) error {
	header := requestHeader{method: raftType}
	sz := batch.SizeUpperLimit()
	var buf []byte
	if len(c.payload) < sz {
		buf = make([]byte, sz)
	} else {
		buf = c.payload
	}
	n, err := batch.MarshalTo(buf)
	if err != nil {
		panic(err)
	}

	addr, err := net.ResolveUDPAddr("udp", c.target)
	if err != nil {
		return err
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}

	return writeMessage(conn, header, buf[:n], c.header)
}

// GetSnapshotConnection returns a new raftio.IConnection for sending raft
// snapshots.
func (g *UDPTransport) GetSnapshotConnection(ctx context.Context,
	target string) (raftio.ISnapshotConnection, error) {
	conn, err := g.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewUDPSnapshotConnection(conn), nil
}

// SendSnapshotChunk sends the specified snapshot chunk to remote node.
func (c *UDPSnapshotConnection) SendSnapshotChunk(chunk raftpb.SnapshotChunk) error {
	header := requestHeader{method: snapshotType}
	sz := chunk.Size()
	buf := make([]byte, sz)
	n, err := chunk.MarshalTo(buf)
	if err != nil {
		panic(err)
	}
	return writeMessage(c.conn, header, buf[:n], c.header)
}

// Name returns a human readable name of the TCP transport module.
func (g *UDPTransport) Name() string {
	return UDPRaftRPCName
}

func (g *UDPTransport) getConnection(ctx context.Context,
	target string) (net.Conn, error) {
	//timeout := time.Duration(getDialTimeoutSecond()) * time.Second

	// conn, err := net.DialTimeout("udp", target, timeout)
	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	
	tlsConfig, err := g.nhConfig.GetClientTLSConfig(target)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		//conn = tls.Client(conn, tlsConfig)
		//tt := time.Now().Add(tlsHandshackTimeout)
		//if err := conn.SetDeadline(tt); err != nil {
		//	return nil, err
		//}
		//if err := conn.(*tls.Conn).Handshake(); err != nil {
		//	return nil, err
		//}
	}
	return conn, nil
}
