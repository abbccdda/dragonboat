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
	"bytes"
	"context"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/internal/utils/syncutil"
	"github.com/lni/dragonboat/raftio"
	"github.com/lni/dragonboat/raftpb"
	"hash/crc32"
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
func NewUDPConnection(conn net.Conn, target string) *UDPConnection {
	return &UDPConnection{
		conn:    conn,
		header:  make([]byte, requestHeaderSize),
		payload: make([]byte, perConnBufSize),
		target: target,
	}
}


// UDPSnapshotConnection is the connection for sending raft snapshot chunks to
// remote nodes.
type UDPSnapshotConnection struct {
	conn   *net.UDPConn
	header []byte
}

// NewUDPSnapshotConnection creates and returns a new snapshot connection.
func NewUDPSnapshotConnection(conn *net.UDPConn) *UDPSnapshotConnection {
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

	//addr, err := net.ResolveUDPAddr("udp", address)
	//if err != nil {
	//	return err
	//}
	//conn, err := net.ListenUDP("udp", addr)
	//
	//if err != nil {
	//	plog.Panicf("failed to new a stoppable listener, %v", err)
	//}

	pc, err := net.ListenPacket("udp", address)
	if err != nil {
		return err
	}

	g.stopper.RunWorker(func() {
		//for {
			var once sync.Once
			closeFn := func() {
				once.Do(func() {
					if err := pc.Close(); err != nil {
						plog.Errorf("failed to close the listener packet %v", err)
					}
				})
			}
			g.stopper.RunWorker(func() {
				<-g.stopper.ShouldStop()
				closeFn()
			})
			g.stopper.RunWorker(func() {
				if err := g.serveConn(pc); err != nil {
					plog.Errorf("encountered error while serving connection: %v", err)
				}
				closeFn()
			})
		//}
	})
	return nil
}

func (g *UDPTransport) serveConn(pc net.PacketConn) error {
	plog.Errorf("serving a connection!")
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

		//tt := time.Now().Add(magicNumberDuration)
		//if err := pc.SetReadDeadline(tt); err != nil {
		//}

		if n, _, err := pc.ReadFrom(magicNum); err != nil {
			plog.Errorf("err reading full buffer from udp %v", err)
			return err
		} else {
			plog.Errorf("successfully read magic num %v with total %v read", magicNum, n)
		}

		if !bytes.Equal(magicNum, magicNumber[:]) {
			plog.Errorf("invalid magic number")
			return nil
		} else {
			plog.Errorf("Find match magic number")
		}

		plog.Errorf("Reach L179")

		//tt = time.Now().Add(headerDuration)
		//if err := pc.SetReadDeadline(tt); err != nil {
		//	return err
		//}
		if _, _, err := pc.ReadFrom(header); err != nil {
			plog.Errorf("failed to get the header %v", err)
			return err
		} else {
			plog.Errorf("found correct header %v", header)
		}
		plog.Errorf("Reach L192")


		rheader, buf, err := readUDPMessage(header, tbuf, pc)
		if err != nil {
			plog.Errorf("Encounter error while reading header %v \n", header)
			//plog.Errorf("actual full data looks like %v", fullBuf[:])
			return err
		}

		if rheader.method == raftType {
			batch := raftpb.MessageBatch{}
			if err := batch.Unmarshal(buf); err != nil {
				return err
			}
			g.requestHandler(batch)
		} else {
			chunk := raftpb.SnapshotChunk{}
			if err := chunk.Unmarshal(buf); err != nil {
				return err
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

func readUDPMessage(header []byte, rbuf []byte, pc net.PacketConn) (requestHeader, []byte, error) {
	rheader := requestHeader{}
	if !rheader.decode(header) {
		plog.Errorf("invalid header")
		return requestHeader{}, nil, ErrBadMessage
	}
	if rheader.size == 0 {
		plog.Errorf("invalid payload length")
		return requestHeader{}, nil, ErrBadMessage
	}
	var buf []byte
	if rheader.size > uint32(len(rbuf)) {
		buf = make([]byte, rheader.size)
	} else {
		buf = rbuf[:rheader.size]
	}


	received := 0
	var recvBuf []byte
	if rheader.size < uint32(recvBufSize) {
		recvBuf = buf[:rheader.size]
	} else {
		recvBuf = buf[:recvBufSize]
	}
	toRead := rheader.size
	for toRead > 0 {
		tt := time.Now().Add(readDuration)
		if err := pc.SetReadDeadline(tt); err != nil {
			return requestHeader{}, nil, err
		}
		if _, _, err := pc.ReadFrom(recvBuf); err != nil {
			return requestHeader{}, nil, err
		}
		toRead -= uint32(len(recvBuf))
		received += len(recvBuf)
		if toRead < uint32(recvBufSize) {
			recvBuf = buf[received : uint32(received)+toRead]
		} else {
			recvBuf = buf[received : received+int(recvBufSize)]
		}
	}
	if uint32(received) != rheader.size {
		panic("unexpected size")
	}
	//if crc32.ChecksumIEEE(buf) != rheader.crc {
	//	plog.Errorf("invalid payload checksum")
	//	return requestHeader{}, nil, ErrBadMessage
	//}
	return rheader, buf, nil
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
	return NewUDPConnection(conn, target), nil
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

	return writeUDPMessage(conn, header, buf[:n], c.header)
}


func writeUDPMessage(conn *net.UDPConn,
	header requestHeader, buf []byte, headerBuf []byte) error {
	crc := crc32.ChecksumIEEE(buf)
	header.size = uint32(len(buf))
	header.crc = crc
	headerBuf = header.encode(headerBuf)
	tt := time.Now().Add(magicNumberDuration).Add(headerDuration).Add(writeDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}

	//fullLength := len(magicNumber) + requestHeaderSize + int(payloadBufferSize)
	//
	//fullBuf := make([]byte, 0)
	//fullBuf = append(fullBuf, magicNumber[:]...)
	//fullBuf = append(fullBuf, headerBuf...)
	//fullBuf = append(fullBuf, buf...)


	if _, err := conn.Write(magicNumber[:]); err != nil {
		plog.Errorf("could not write magic number", err)
		return err
	}
	if _, err := conn.Write(headerBuf); err != nil {
		plog.Errorf("could not write header buf", err)
		return err
	}


	sent := 0
	bufSize := int(recvBufSize)
	for sent < len(buf) {
		if sent+bufSize > len(buf) {
			bufSize = len(buf) - sent
		}
		if err := conn.SetWriteDeadline(tt); err != nil {
			return err
		}
		if _, err := conn.Write(buf[sent : sent+bufSize]); err != nil {
			return err
		}
		sent += bufSize
	}
	if sent != len(buf) {
		plog.Panicf("sent %d, buf len %d", sent, len(buf))
	}

	//plog.Errorf("get full buf to be written: %v", fullBuf)
	//
	//if _, err := conn.Write(fullBuf); err != nil {
	//	return err
	//}

	plog.Errorf("finished sending message batch")

	return nil
}

// GetSnapshotConnection returns a new raftio.IConnection for sending raft
// snapshots.
func (g *UDPTransport) GetSnapshotConnection(ctx context.Context,
	target string) (raftio.ISnapshotConnection, error) {
	conn, err := g.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewUDPSnapshotConnection(conn.(*net.UDPConn)), nil
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
	return writeUDPMessage(c.conn, header, buf[:n], c.header)
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
