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
	"github.com/lni/dragonboat/internal/settings"
	"github.com/lni/dragonboat/internal/utils/leaktest"
	"testing"
)

func TestUDPMessageCanBeSent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testMessageCanBeSent(t, false, settings.MaxProposalPayloadSize*2, NewUDPTransport)
	testMessageCanBeSent(t, false, recvBufSize/2, NewUDPTransport)
	testMessageCanBeSent(t, false, recvBufSize+1, NewUDPTransport)
	testMessageCanBeSent(t, false, perConnBufSize+1, NewUDPTransport)
	testMessageCanBeSent(t, false, perConnBufSize/2, NewUDPTransport)
	testMessageCanBeSent(t, false, 1, NewUDPTransport)
	testMessageCanBeSent(t, true, settings.MaxProposalPayloadSize*2, NewUDPTransport)
	testMessageCanBeSent(t, true, recvBufSize/2, NewUDPTransport)
	testMessageCanBeSent(t, true, recvBufSize+1, NewUDPTransport)
	testMessageCanBeSent(t, true, perConnBufSize+1, NewUDPTransport)
	testMessageCanBeSent(t, true, perConnBufSize/2, NewUDPTransport)
	testMessageCanBeSent(t, true, 1, NewUDPTransport)
}