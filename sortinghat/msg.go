package sortinghat

import ()

// A manually-unpacked version of (id, bits).
// This is in its own struct for easy printing.
type MsgHdr struct {
	PacketLength uint16
	SequenceNo   uint16
	MsgType      byte
	Reserved     byte
	MsgSize      uint16
}

// The layout of a DNS message.
type Msg struct {
	MsgHdr
}

func (msg *Msg) Pack() (m []byte, err error) {
	return nil, nil // FIXME
}

func (msg *Msg) Unpack(m []byte) (err error) {
	return nil // FIXME
}
