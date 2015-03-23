/*
Ring implements a simple ring CRDT.
*/
package ring

import (
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/zettio/weave/ipam"
	"github.com/zettio/weave/router"
	"net"
	"sort"
)

type entry struct {
	Token     uint32          // The start of this range
	Peer      router.PeerName // Who owns this range
	Tombstone uint32          // Timestamp when this entry was tombstone; 0 means live
	Version   uint32          // Version of this range
}

// For compatibility with sort.Interface
type entries []entry

func (es entries) Len() int           { return len(es) }
func (es entries) Less(i, j int) bool { return es[i].Token < es[j].Token }
func (es entries) Swap(i, j int)      { panic("Should never be swapping entries!") }

type Ring struct {
	Start, End uint32          // [min, max) tokens in this ring
	Peername   router.PeerName // name of peer owning this ring instance
	Entries    entries         // list of entries sorted by token
}

func assert(test bool, message string) {
	if !test {
		panic(message)
	}
}

func (r *Ring) assertInvariants() {
	err := r.checkInvariants()
	if err != nil {
		panic(err.Error())
	}
}

func (r *Ring) checkInvariants() error {
	if !sort.IsSorted(r.Entries) {
		return errors.New("Ring not sorted")
	}

	// Check no token appears twice

	if len(r.Entries) == 0 {
		return nil
	}

	if r.Entries[0].Token < r.Start {
		return errors.New("First token is out of range")
	}

	if r.Entries[len(r.Entries)-1].Token >= r.End {
		return errors.New("Last token is out of range")
	}

	return nil
}

func (r *Ring) insertAt(i int, e entry) {
	r.assertInvariants()

	if i < len(r.Entries) && r.Entries[i].Token == e.Token {
		panic("Trying to insert an existing token!")
	}

	r.Entries = append(r.Entries, entry{})
	copy(r.Entries[i+1:], r.Entries[i:])
	r.Entries[i] = e

	r.assertInvariants()
}

// New creates an empty ring belonging to peer.
func New(start, end net.IP, peer router.PeerName) Ring {
	return Ring{ipam.Ip4int(start), ipam.Ip4int(end), peer, make([]entry, 0)}
}

// Is token between entries at i and j?
// NB i and j can overflow and will wrap
// NBB if entries[i].token == token, this will return true
func (r *Ring) between(token uint32, i, j int) bool {
	assert(i < j, "Start and end must be in order")

	first := r.Entries[i%len(r.Entries)]
	second := r.Entries[j%len(r.Entries)]

	switch {
	case first.Token == second.Token:
		// This implies there is only one token
		// on the ring (i < j and i.token == j.token)
		// In which case everything is between.
		return true

	case first.Token < second.Token:
		return first.Token <= token && token < second.Token

	case second.Token < first.Token:
		return first.Token <= token || token < second.Token
	}

	assert(false, "Should never get here.")
	return true
}

// Grant range [start, end) to peer
// Note, due to wrapping, end can be less than start
func (r *Ring) GrantRangeToHost(startIP, endIP net.IP, peer router.PeerName) {
	r.assertInvariants()

	start, end := ipam.Ip4int(startIP), ipam.Ip4int(endIP)
	assert(r.Start <= start && start <= r.End, "Trying to grant range outside of subnet")
	assert(r.Start <= end && end <= r.End, "Trying to grant range outside of subnet")
	assert(len(r.Entries) > 0, "Cannot grant if ring is empty!")

	// Look for the start entry
	i := sort.Search(len(r.Entries), func(j int) bool {
		return r.Entries[j].Token >= start
	})

	// Is start already owned by us, in which case we need
	// to change the token and update version
	if i < len(r.Entries) && r.Entries[i].Token == start {
		entry := r.Entries[i]
		assert(entry.Peer == r.Peername, "Trying to mutate entry I don't own")
		entry.Peer = peer
		entry.Tombstone = 0
		entry.Version++
	} else {
		// Otherwise, these isn't a token here, we need to
		// find the preceeding token and check we own it (being careful for wrapping)
		j := i - 1
		assert(r.between(start, j, i), "??")
		previous := r.Entries[j%len(r.Entries)]
		assert(previous.Peer == r.Peername, "Trying to mutate range I don't own")

		r.insertAt(i, entry{start, peer, 0, 0})
	}

	r.assertInvariants()

	// Now we need to deal with the end token.  There are 3 cases:
	//   i.   the next token is equals to the end of the range
	//        => we don't need to do anything
	//   ii.  the end is between this token and the next,
	//        => we need to insert a token such that
	//        we claim this bit on the end.
	//   iii. the end is not between this token and next
	//        => this is an error, we'll be splitting someone
	//        elses ranges.

	k := i + 1
	nextEntry := r.Entries[k%len(r.Entries)]
	if nextEntry.Token == end {
		// That was easy
		return
	} else {
		assert(r.between(end, i, k), "End spans another token")
		r.insertAt(k, entry{end, r.Peername, 0, 0})
		r.assertInvariants()
	}
}

func (r *Ring) merge(gossip Ring) error {
	r.assertInvariants()

	if err := gossip.checkInvariants(); err != nil {
		return err
	}

	if r.Start != gossip.Start || r.End != gossip.End {
		return errors.New("Cannot merge gossip for different subnet!")
	}

	// We special case us having an empty ring -
	// in this case we might be coming up in an existing
	// network and be given some ranges we might have forgotten
	// abouts.  Assertions below would fail as it would appear
	// other nodes are gossiping tokens in ranges we own.
	if len(r.Entries) == 0 {
		r.Entries = gossip.Entries
		return nil
	}

	// first count number of distinct tokens
	tokens := make(map[uint32]bool)
	for _, entry := range r.Entries {
		tokens[entry.Token] = true
	}
	for _, entry := range gossip.Entries {
		tokens[entry.Token] = true
	}

	// Merge two entries with the same token
	mergeEntry := func(existingEntry, newEntry *entry) (*entry, error) {
		assert(existingEntry.Token == newEntry.Token, "WTF")
		switch {
		case existingEntry.Version == newEntry.Version:
			if existingEntry != newEntry {
				return nil, errors.New("Recieved invalid state update!")
			}
			return existingEntry, nil

		case existingEntry.Version < newEntry.Version:
			// A new token it getting inserted
			if existingEntry.Peer == r.Peername {
				return nil, errors.New("Recieved new version for entry I own!")
			}
			return newEntry, nil

		case existingEntry.Version > newEntry.Version:
			return newEntry, nil
		}
		// This should never be hit (switch covers all cases) but go doesn't detect that.
		return nil, nil
	}

	// Make new slice for result; iterate over
	// existing state and new state merging into
	// result and checking for invariants.
	result := make([]entry, len(tokens))
	i, j := 0, 0

	// Owner is the owner of the largest token
	// TODO: What to do if ring is empty?
	var currentOwner router.PeerName
	last := func(es []entry) entry { return es[len(es)-1] }
	if last(r.Entries).Token > last(gossip.Entries).Token {
		currentOwner = last(r.Entries).Peer
	} else {
		currentOwner = last(gossip.Entries).Peer
	}

	for k := range result {
		switch {
		case i >= len(r.Entries) || r.Entries[i].Token > gossip.Entries[j].Token:
			// A new token it getting inserted
			if currentOwner == r.Peername {
				return errors.New("Recieved new entry in my range!")
			}
			result[k] = gossip.Entries[j]
			j++

		case j >= len(gossip.Entries) || r.Entries[i].Token < gossip.Entries[j].Token:
			result[k] = r.Entries[i]
			i++

		case r.Entries[i].Token == gossip.Entries[j].Token:
			if entry, err := mergeEntry(&r.Entries[i], &gossip.Entries[j]); err != nil {
				return err
			} else {
				result[k] = *entry
			}
			i++
			j++
		}
		currentOwner = result[k].Peer
	}
	assert(i == len(r.Entries) && j == len(gossip.Entries), "WTF")

	r.Entries = result
	r.assertInvariants()
	return nil
}

func (r *Ring) OnGossipBroadcast(msg []byte) error {
	reader := bytes.NewReader(msg)
	decoder := gob.NewDecoder(reader)
	gossipedRing := Ring{}

	if err := decoder.Decode(&gossipedRing); err != nil {
		return err
	}

	if err := r.merge(gossipedRing); err != nil {
		return err
	}
	return nil
}

func (r *Ring) GossipState() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(r); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// Claim entire ring.  Only works for empty rings.
func (r *Ring) ClaimItAll() {
	assert(len(r.Entries) == 0, "Cannot bootstrap ring with entries in it!")

	r.insertAt(0, entry{r.Start, r.Peername, 0, 0})

	r.assertInvariants()
}
