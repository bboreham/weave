/*
Ring implements a simple ring CRDT.

TODO: merge consequtively owned ranges
TODO: implement tombstones
*/
package ring

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/zettio/weave/ipam/utils"
	"github.com/zettio/weave/router"
	"math/rand"
	"net"
	"sort"
)

type entry struct {
	Token     uint32          // The start of this range
	Peer      router.PeerName // Who owns this range
	Tombstone uint32          // Timestamp when this entry was tombstone; 0 means live
	Version   uint32          // Version of this range
	Free      uint32          // Number of free IPs in this range
}

func (e1 *entry) Equal(e2 *entry) bool {
	return e1.Token == e2.Token && e1.Peer == e2.Peer &&
		e1.Tombstone == e2.Tombstone && e1.Version == e2.Version
}

// For compatibility with sort.Interface
type entries []entry

func (es entries) Len() int           { return len(es) }
func (es entries) Less(i, j int) bool { return es[i].Token < es[j].Token }
func (es entries) Swap(i, j int)      { panic("Should never be swapping entries!") }

type Ring struct {
	Start, End uint32          // [min, max) tokens in this ring.  Due to wrapping, min == max (effectively)
	Peername   router.PeerName // name of peer owning this ring instance
	Entries    entries         // list of entries sorted by token
}

func (r *Ring) assertInvariants() {
	err := r.checkInvariants()
	if err != nil {
		panic(err.Error())
	}
}

// Errors returned by merge
var (
	ErrNotSorted        = errors.New("Ring not sorted")
	ErrTokenRepeated    = errors.New("Token appears twice in ring")
	ErrTokenOutOfRange  = errors.New("Token is out of range")
	ErrDifferentSubnets = errors.New("Cannot merge gossip for different subnet!")
	ErrNewerVersion     = errors.New("Recieved new version for entry I own!")
	ErrInvalidEntry     = errors.New("Recieved invalid state update!")
	ErrEntryInMyRange   = errors.New("Recieved new entry in my range!")
	ErrNoFreeSpace      = errors.New("No free space found!")
)

func (r *Ring) checkInvariants() error {
	if !sort.IsSorted(r.Entries) {
		return ErrNotSorted
	}

	// Check no token appears twice
	tokens := make(map[uint32]bool)
	for _, entry := range r.Entries {
		if _, ok := tokens[entry.Token]; ok {
			return ErrTokenRepeated
		}
		tokens[entry.Token] = true
	}

	if len(r.Entries) == 0 {
		return nil
	}

	if r.Entries[0].Token < r.Start {
		return ErrTokenOutOfRange
	}

	if r.Entries[len(r.Entries)-1].Token >= r.End {
		return ErrTokenOutOfRange
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
func New(startIP, endIP net.IP, peer router.PeerName) *Ring {
	start, end := utils.Ip4int(startIP), utils.Ip4int(endIP)
	utils.Assert(start <= end, "Start needs to be less than end!")

	return &Ring{start, end, peer, make([]entry, 0)}
}

// Is token between entries at i and j?
// NB i and j can overflow and will wrap
// NBB if entries[i].token == token, this will return true
func (r *Ring) between(token uint32, i, j int) bool {
	utils.Assert(i < j, "Start and end must be in order")

	first := r.Entries[i%len(r.Entries)]
	second := r.Entries[j%len(r.Entries)]

	switch {
	case first.Token == second.Token:
		// This implies there is only one token
		// on the ring (i < j and i.token == j.token)
		// In which case everything is between, expect
		// this one token
		return token != first.Token

	case first.Token < second.Token:
		return first.Token <= token && token < second.Token

	case second.Token < first.Token:
		return first.Token <= token || token < second.Token
	}

	panic("Should never get here - switch covers all possibilities.")
}

// Grant range [start, end) to peer
// Note, due to wrapping, end can be less than start
func (r *Ring) GrantRangeToHost(startIP, endIP net.IP, peer router.PeerName) {
	r.assertInvariants()

	start, end := utils.Ip4int(startIP), utils.Ip4int(endIP)
	utils.Assert(r.Start <= start && start < r.End, "Trying to grant range outside of subnet")
	utils.Assert(r.Start < end && end <= r.End, "Trying to grant range outside of subnet")
	utils.Assert(len(r.Entries) > 0, "Cannot grant if ring is empty!")

	// Look for the start entry
	i := sort.Search(len(r.Entries), func(j int) bool {
		return r.Entries[j].Token >= start
	})

	// Is start already owned by us, in which case we need
	// to change the token and update version
	if i < len(r.Entries) && r.Entries[i].Token == start {
		entry := &r.Entries[i]
		utils.Assert(entry.Peer == r.Peername, "Trying to mutate entry I don't own")
		entry.Peer = peer
		entry.Tombstone = 0
		entry.Version++
	} else {
		// Otherwise, these isn't a token here, we need to
		// find the preceeding token and check we own it (being careful for wrapping)
		j := i - 1
		utils.Assert(r.between(start, j, i), "??")
		previous := r.Entries[j%len(r.Entries)]
		utils.Assert(previous.Peer == r.Peername, "Trying to mutate range I don't own")

		r.insertAt(i, entry{Token: start, Peer: peer})
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
	nextEntry := &r.Entries[k%len(r.Entries)]

	// End needs wrapping
	end = r.Start + ((end - r.Start) % (r.End - r.Start))

	if nextEntry.Token == end {
		// That was easy
		return
	} else {
		utils.Assert(r.between(end, i, k), "End spans another token")
		r.insertAt(k, entry{Token: end, Peer: r.Peername})
		r.assertInvariants()
	}
}

// Merge the given ring into this ring.
func (r *Ring) merge(gossip Ring) error {
	r.assertInvariants()

	// Don't panic when checking the gossiped in ring.
	// In this case just return any error found.
	if err := gossip.checkInvariants(); err != nil {
		return err
	}

	if r.Start != gossip.Start || r.End != gossip.End {
		return ErrDifferentSubnets
	}

	// Special case gossip ring being empty - not much to do...
	if len(gossip.Entries) == 0 {
		return nil
	}

	// We special case us having an empty ring -
	// in this case we might be coming up in an existing
	// network and be given some ranges we might have forgotten
	// abouts.  Assertions below would fail as it would appear
	// other nodes are gossiping tokens in ranges we own.
	if len(r.Entries) == 0 {
		r.Entries = make([]entry, len(gossip.Entries))
		copy(r.Entries, gossip.Entries)
		return nil
	}

	// First count number of distinct tokens to build the result with
	tokens := make(map[uint32]bool)
	for _, entry := range r.Entries {
		tokens[entry.Token] = true
	}
	for _, entry := range gossip.Entries {
		tokens[entry.Token] = true
	}

	// mergeEntry merges two entries with the same token
	// returns the merged entry, flag indicating if this was
	// from the gossiped ring, and an error
	mergeEntry := func(existingEntry, newEntry entry) (entry, bool, error) {
		utils.Assert(existingEntry.Token == newEntry.Token, "WTF")
		switch {
		case existingEntry.Version == newEntry.Version:
			if !existingEntry.Equal(&newEntry) {
				return entry{}, false, ErrInvalidEntry
			}
			return existingEntry, false, nil

		case existingEntry.Version < newEntry.Version:
			// A new token it getting inserted
			if existingEntry.Peer == r.Peername {
				return entry{}, false, ErrNewerVersion
			}
			return newEntry, true, nil

		case existingEntry.Version > newEntry.Version:
			return existingEntry, false, nil
		}

		panic("Should never get here - switch covers all possibilities.")
	}

	// Make new slice for result; iterate over
	// existing state and new state merging into
	// result and checking for invariants.
	result := make([]entry, len(tokens))
	i, j := 0, 0

	// Owner is the owner of the largest token
	// We know are ring isn't empty at this point.
	var currentOwner router.PeerName
	last := func(es []entry) entry { return es[len(es)-1] }
	if last(r.Entries).Token > last(gossip.Entries).Token {
		currentOwner = last(r.Entries).Peer
	} else {
		currentOwner = last(gossip.Entries).Peer
	}

	// Consumes an entry from the gossiped ring
	// and puts it in out ring at k
	consumeGossip := func(k int) error {
		if currentOwner == r.Peername {
			return ErrEntryInMyRange
		}
		result[k] = gossip.Entries[j]
		j++

		// Don't update current owner if we've consumed a
		// gossip entry, as we might have been given some
		// space, and this would cause us to reject the
		// next token
		currentOwner = router.UnknownPeerName
		return nil
	}

	// Consumes an entry from our local state
	consumeLocal := func(k int) {
		result[k] = r.Entries[i]
		i++
		currentOwner = result[k].Peer
	}

	// Merges an entry from the gossiped ring
	// with our state
	consumeBoth := func(k int) error {
		entry, remote, err := mergeEntry(r.Entries[i], gossip.Entries[j])
		if err != nil {
			return err
		} else {
			result[k] = entry
		}
		i++
		j++

		// Don't update current owner if we've consumed a
		// gossip entry, as we might have been given some
		// space, and this would cause us to reject the
		// next token
		if !remote {
			currentOwner = result[k].Peer
		} else {
			currentOwner = router.UnknownPeerName
		}
		return nil
	}

	for k := range result {
		// Ordering of cases is important here!
		switch {
		case i >= len(r.Entries):
			if err := consumeGossip(k); err != nil {
				return err
			}

		case j >= len(gossip.Entries):
			consumeLocal(k)

		case r.Entries[i].Token > gossip.Entries[j].Token:
			if err := consumeGossip(k); err != nil {
				return err
			}

		case r.Entries[i].Token < gossip.Entries[j].Token:
			consumeLocal(k)

		case r.Entries[i].Token == gossip.Entries[j].Token:
			if err := consumeBoth(k); err != nil {
				return err
			}
		}
	}
	utils.Assert(i == len(r.Entries) && j == len(gossip.Entries), "WTF")

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

func (r *Ring) Empty() bool {
	return len(r.Entries) == 0
}

// Return type for OwnedRanges.
// NB End can be less than Start for ranges
// which cross 0.
type Range struct {
	Start, End net.IP // [Start, End) of range I own
}

type RangeSlice []Range

func (r1 Range) Equal(r2 Range) bool {
	return r1.Start.Equal(r2.Start) && r1.End.Equal(r2.End)
}

func (rs1 RangeSlice) Equal(rs2 []Range) bool {
	if len(rs1) != len(rs2) {
		return false
	}

	for i := 0; i < len(rs1); i++ {
		if !rs1[i].Equal(rs2[i]) {
			return false
		}
	}

	return true
}

// Return slice of Ranges indicating which
// ranges are owned by this peer.
func (r *Ring) OwnedRanges() RangeSlice {
	// Cannot return more the entries + 1 results
	// +1 for splitting results spanning 0
	result := make([]Range, len(r.Entries)+1)
	j := 0 // index into above slice

	// Finally iterate through entries again
	// filling in result as we go.
	for i, entry := range r.Entries {
		if entry.Peer != r.Peername {
			continue
		}

		// next logical token in ring; be careful of
		// wrapping the index
		nextEntry := r.Entries[(i+1)%len(r.Entries)]

		switch {
		case nextEntry.Token == r.Start:
			// be careful here; if end token == start (ie last)
			// entry on ring, we want to actually use r.End
			result[j] = Range{Start: utils.Intip4(entry.Token),
				End: utils.Intip4(r.End)}
			j++

		case nextEntry.Token <= entry.Token:
			// We wrapped; want to split around 0
			// First shuffle everything up as we want results to be sorted
			copy(result[1:j+1], result[:j])
			result[0] = Range{Start: utils.Intip4(r.Start),
				End: utils.Intip4(nextEntry.Token)}
			result[j+1] = Range{Start: utils.Intip4(entry.Token),
				End: utils.Intip4(r.End)}
			j = j + 2

		default:
			result[j] = Range{Start: utils.Intip4(entry.Token),
				End: utils.Intip4(nextEntry.Token)}
			j++
		}
	}

	return result[:j]
}

// Claim entire ring.  Only works for empty rings.
func (r *Ring) ClaimItAll() {
	utils.Assert(len(r.Entries) == 0, "Cannot bootstrap ring with entries in it!")

	r.insertAt(0, entry{Token: r.Start, Peer: r.Peername, Free: r.End - r.Start})

	r.assertInvariants()
}

func (r *Ring) String() string {
	var buffer bytes.Buffer
	for _, entry := range r.Entries {
		fmt.Fprintf(&buffer, "%s -> %s (%d, %d, %d)\n", utils.Intip4(entry.Token),
			entry.Peer, entry.Tombstone, entry.Version, entry.Free)
	}
	return buffer.String()
}

func (r *Ring) ReportFree(startIP net.IP, free uint32) {
	start := utils.Ip4int(startIP)

	// Look for entry
	i := sort.Search(len(r.Entries), func(j int) bool {
		return r.Entries[j].Token >= start
	})

	utils.Assert(i < len(r.Entries) && r.Entries[i].Token == start &&
		r.Entries[i].Peer == r.Peername, "Trying to report free on space I don't own")

	r.Entries[i].Free = free
	r.Entries[i].Version++
}

func (r *Ring) ChoosePeerToAskForSpace() (result router.PeerName, err error) {
	// Construct total free space and number of ranges per peer
	totalSpacePerPeer := make(map[router.PeerName]int)
	numRangesPerPeer := make(map[router.PeerName]int)
	for _, entry := range r.Entries {
		// Ignore ranges with no free space
		if entry.Free <= 0 {
			continue
		}

		// Don't talk to yourself
		if entry.Peer == r.Peername {
			continue
		}

		if sum, ok := totalSpacePerPeer[entry.Peer]; ok {
			totalSpacePerPeer[entry.Peer] = sum + int(entry.Free)
			numRangesPerPeer[entry.Peer] = numRangesPerPeer[entry.Peer] + 1
		} else {
			totalSpacePerPeer[entry.Peer] = int(entry.Free)
			numRangesPerPeer[entry.Peer] = 1
		}
	}

	utils.Assert(len(totalSpacePerPeer) == len(numRangesPerPeer), "WFT")

	if len(totalSpacePerPeer) <= 0 {
		err = ErrNoFreeSpace
		return
	}

	// Construct average free space per range per peer
	type choice struct {
		peer   router.PeerName
		weight int
	}
	sum := 0
	choices := make([]choice, len(totalSpacePerPeer))
	i := 0
	for peer, free := range totalSpacePerPeer {
		average := (free / numRangesPerPeer[peer])
		choices[i] = choice{peer, average}
		sum += average
		i++
	}

	// Pick random peer, weighted by average free space
	rn := rand.Intn(sum)
	for _, c := range choices {
		rn -= c.weight
		if rn < 0 {
			result = c.peer
			return
		}
	}

	panic("Should never reach this point")
}
