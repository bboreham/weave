/*
Package ring implements a simple ring CRDT.

TODO: merge consequtively owned ranges
TODO: implement tombstones
*/
package ring

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"time"

	"github.com/zettio/weave/ipam/utils"
	"github.com/zettio/weave/router"
)

// Ring represents the ring itself
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
	ErrNewerVersion     = errors.New("Received new version for entry I own!")
	ErrInvalidEntry     = errors.New("Received invalid state update!")
	ErrEntryInMyRange   = errors.New("Received new entry in my range!")
	ErrNoFreeSpace      = errors.New("No free space found!")
	ErrTooMuchFreeSpace = errors.New("Entry reporting too much free space!")
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

	// Check tokens are in range
	if r.Entries.entry(0).Token < r.Start {
		return ErrTokenOutOfRange
	}

	if r.Entries.entry(-1).Token >= r.End {
		return ErrTokenOutOfRange
	}

	// Check all the freespaces are in range
	// NB for this check, we ignore tombstones
	for i, entry := range r.Entries.filteredEntries() {
		next := r.Entries.entry(i + 1)
		distance := r.distance(entry.Token, next.Token)

		if entry.Free > distance {
			return ErrTooMuchFreeSpace
		}
	}

	return nil
}

// New creates an empty ring belonging to peer.
func New(startIP, endIP net.IP, peer router.PeerName) *Ring {
	start, end := utils.IP4int(startIP), utils.IP4int(endIP)
	utils.Assert(start <= end, "Start needs to be less than end!")

	return &Ring{start, end, peer, make([]*entry, 0)}
}

// Returns the distance between two tokens on this ring, deali
// with ranges with cross the origin
func (r *Ring) distance(start, end uint32) uint32 {
	if end > start {
		return end - start
	}

	return (r.End - start) + (end - r.Start)
}

// GrantRangeToHost modifies the ring such that range [start, end)
// is assigned to peer.  This may insert upto two new tokens.
// Note, due to wrapping, end can be less than start
func (r *Ring) GrantRangeToHost(startIP, endIP net.IP, peer router.PeerName) {
	r.assertInvariants()

	start, end := utils.IP4int(startIP), utils.IP4int(endIP)
	utils.Assert(r.Start <= start && start < r.End, "Trying to grant range outside of subnet")
	utils.Assert(r.Start < end && end <= r.End, "Trying to grant range outside of subnet")
	utils.Assert(len(r.Entries) > 0, "Cannot grant if ring is empty!")

	// How much space will the new range have?
	var newFree = r.distance(start, end)
	utils.Assert(newFree > 0, "Cannot create zero-sized ranges")

	// Look for the start entry
	i := sort.Search(len(r.Entries), func(j int) bool {
		return r.Entries[j].Token >= start
	})

	// Is start already owned by us, in which case we need
	// to change the token and update version
	if i < len(r.Entries) && r.Entries[i].Token == start {
		entry := r.Entries[i]
		utils.Assert(entry.Peer == r.Peername, "Trying to mutate entry I don't own")
		entry.Peer = peer
		entry.Tombstone = 0
		entry.Version++
		entry.Free = newFree
	} else {
		// Otherwise, these isn't a token here, we need to
		// find the preceeding token and check we own it (being careful for wrapping)
		j := i - 1
		utils.Assert(r.Entries.between(start, j, i), "??")

		previous := r.Entries.entry(j)
		utils.Assert(previous.Peer == r.Peername, "Trying to mutate range I don't own")

		// Reset free on previous token; may over estimate, but thats fine
		previous.Free = r.distance(previous.Token, start)
		previous.Version++

		r.Entries.insert(entry{Token: start, Peer: peer, Free: newFree})
	}

	r.assertInvariants()

	// Now we need to deal with the end token.  There are 3 cases:
	//   i.   the next token is equal to the end of the range
	//        => we don't need to do anything
	//   ii.  the end is between this token and the next,
	//        => we need to insert a token such that
	//        we claim this bit on the end.
	//   iii. the end is not between this token and next
	//        => this is an error, we'll be splitting someone
	//        else's ranges.

	k := i + 1
	nextEntry := r.Entries.entry(k)

	// There is a special case when end == ring.End
	if end == r.End {
		end = r.Start
	}

	// Case i
	if nextEntry.Token == end {
		// That was easy
		return
	}

	// Case ii (case iii should never happen)
	utils.Assert(r.Entries.between(end, i, k), "End spans another token")
	distance := r.distance(end, r.Entries.entry(k).Token)
	r.Entries.insert(entry{Token: end, Peer: r.Peername, Free: distance})
	r.assertInvariants()
}

// Merge the given ring into this ring.
func (r *Ring) merge(gossip Ring) error {
	r.assertInvariants()
	defer r.assertInvariants()

	// Don't panic when checking the gossiped in ring.
	// In this case just return any error found.
	if err := gossip.checkInvariants(); err != nil {
		return err
	}

	if r.Start != gossip.Start || r.End != gossip.End {
		return ErrDifferentSubnets
	}

	var result entries
	addToResult := func(e entry) { result = append(result, &e) }

	var mine, theirs *entry
	// i is index into r.Entries; j is index into gossip.Entries
	var i, j int
	for i < len(r.Entries) && j < len(gossip.Entries) {
		mine, theirs = r.Entries[i], gossip.Entries[j]
		switch {
		case mine.Token < theirs.Token:
			addToResult(*mine)
			i++
		case mine.Token > theirs.Token:
			// insert, checking that a range owned by us hasn't been split
			if r.Entries.entry(i-1).Peer == r.Peername && theirs.Peer != r.Peername {
				return ErrEntryInMyRange
			}
			addToResult(*theirs)
			j++
		case mine.Token == theirs.Token:
			// merge
			switch {
			case mine.Version > theirs.Version:
				addToResult(*mine)
			case mine.Version == theirs.Version:
				if !mine.Equal(theirs) {
					return ErrInvalidEntry
				}
				addToResult(*mine)
			case mine.Version < theirs.Version:
				if mine.Peer == r.Peername { // We shouldn't receive updates to our own tokens
					return ErrNewerVersion
				}
				addToResult(*theirs)
			}
			i++
			j++
		}
	}

	// At this point, either i is at the end of r or j is at the end
	// of gossip, so copy over the remaining entries.

	for ; i < len(r.Entries); i++ {
		mine = r.Entries[i]
		addToResult(*mine)
	}

	for ; j < len(gossip.Entries); j++ {
		theirs = gossip.Entries[j]
		if mine != nil && mine.Peer == r.Peername && theirs.Peer != r.Peername {
			return ErrEntryInMyRange
		}
		addToResult(*theirs)
	}

	r.Entries = result
	return nil
}

// UpdateRing updates the ring with the state in msg
func (r *Ring) UpdateRing(msg []byte) error {
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

// GossipState returns the encoded state of the ring
func (r *Ring) GossipState() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(r); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// Empty returns true if the ring has no entries
// NB this test includes Tombstones.
// TODO reconsider what happens if you delete every node from the ring?
func (r *Ring) Empty() bool {
	return len(r.Entries) == 0
}

// Range is the return type for OwnedRanges.
// NB will never have End < Start
type Range struct {
	Start, End net.IP // [Start, End) of range I own
}

// OwnedRanges returns slice of Ranges indicating which
// ranges are owned by this peer.  Will split ranges which
// span 0 in the ring.
func (r *Ring) OwnedRanges() []Range {
	var (
		result  []Range
		entries = r.Entries.filteredEntries() // We can ignore tombstones in this method
	)
	r.assertInvariants()
	defer r.assertInvariants()

	// Finally iterate through entries again
	// filling in result as we go.
	for i, entry := range entries {
		if entry.Peer != r.Peername {
			continue
		}

		// next logical token in ring; be careful of
		// wrapping the index
		nextEntry := entries.entry(i + 1)

		switch {
		case nextEntry.Token == r.Start:
			// be careful here; if end token == start (ie last)
			// entry on ring, we want to actually use r.End
			result = append(result, Range{Start: utils.IntIP4(entry.Token),
				End: utils.IntIP4(r.End)})

		case nextEntry.Token <= entry.Token:
			// We wrapped; want to split around 0
			// First shuffle everything up as we want results to be sorted
			result = append(result, Range{})
			copy(result[1:], result[:len(result)-1])
			result[0] = Range{Start: utils.IntIP4(r.Start),
				End: utils.IntIP4(nextEntry.Token)}
			result = append(result, Range{Start: utils.IntIP4(entry.Token),
				End: utils.IntIP4(r.End)})

		default:
			result = append(result, Range{Start: utils.IntIP4(entry.Token),
				End: utils.IntIP4(nextEntry.Token)})
		}
	}

	return result
}

// ClaimItAll claims the entire ring for this peer.  Only works for empty rings.
func (r *Ring) ClaimItAll() {
	utils.Assert(len(r.Entries) == 0, "Cannot bootstrap ring with entries in it!")

	// We reserve the first and last address with a special range; this ensures
	// they are never given out by anyone
	// Per RFC1122, don't allocate the first (network) and last (broadcast) addresses
	r.Entries.insert(entry{Token: r.Start + 1, Peer: r.Peername,
		Free: r.End - r.Start - 2})
	r.Entries.insert(entry{Token: r.End - 1, Peer: router.UnknownPeerName})

	r.assertInvariants()
}

func (r *Ring) String() string {
	var buffer bytes.Buffer
	for _, entry := range r.Entries {
		fmt.Fprintf(&buffer, "%s -> %s (%d, %d, %d)\n", utils.IntIP4(entry.Token),
			entry.Peer, entry.Tombstone, entry.Version, entry.Free)
	}
	return buffer.String()
}

// ReportFree is used by the allocator to tell the ring
// how many free ips are in a given ring, so that ChoosePeerToAskForSpace
// can make more intelligent decisions.
func (r *Ring) ReportFree(startIP net.IP, free uint32) {
	start := utils.IP4int(startIP)
	entries := r.Entries.filteredEntries() // We don't want to report free on tombstones

	// Look for entry
	i := sort.Search(len(entries), func(j int) bool {
		return entries[j].Token >= start
	})

	utils.Assert(i < len(entries) && entries[i].Token == start &&
		entries[i].Peer == r.Peername, "Trying to report free on space I don't own")

	// Check we're not reporting more space than the range
	entry, next := entries.entry(i), entries.entry(i+1)
	maxSize := r.distance(entry.Token, next.Token)
	utils.Assert(free <= maxSize, "Trying to report more free space than possible")

	if entries[i].Free == free {
		return
	}

	entries[i].Free = free
	entries[i].Version++
}

// ChoosePeerToAskForSpace chooses a weighted-random peer to ask
// for space.
func (r *Ring) ChoosePeerToAskForSpace() (result router.PeerName, err error) {
	var (
		sum               uint32
		totalSpacePerPeer = make(map[router.PeerName]uint32) // Compute total free space per peer
	)

	// iterate through tokens IGNORING tombstones
	for _, entry := range r.Entries.filteredEntries() {
		utils.Assert(entry.Tombstone == 0, "List shouldn't contain tombstoned entries")

		// Ignore ranges with no free space
		if entry.Free <= 0 {
			continue
		}

		// Don't talk to yourself
		if entry.Peer == r.Peername {
			continue
		}

		totalSpacePerPeer[entry.Peer] += entry.Free
		sum += entry.Free
	}

	if sum == 0 {
		err = ErrNoFreeSpace
		return
	}

	// Pick random peer, weighted by total free space
	rn := rand.Int63n(int64(sum))
	for peername, space := range totalSpacePerPeer {
		rn -= int64(space)
		if rn < 0 {
			return peername, nil
		}
	}

	panic("Should never reach this point")
}

// TombstonePeer will mark all entries associated with this peer as tombstones
func (r *Ring) TombstonePeer(peer router.PeerName, dt int64) {
	utils.Assert(peer != r.Peername, "Cannot tombstone yourself")

	absTimeout := time.Now().Unix() + dt

	for _, entry := range r.Entries {
		if entry.Peer == peer {
			entry.Tombstone = absTimeout
			entry.Version++
		}
	}
}
