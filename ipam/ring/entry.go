package ring

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/zettio/weave/ipam/utils"
	"github.com/zettio/weave/router"
)

// Entry represents entries around the ring
type entry struct {
	Token     uint32          // The start of this range
	Peer      router.PeerName // Who owns this range
	Tombstone int64           // Timestamp when this entry was tombstone; 0 means live
	Version   uint32          // Version of this range
	Free      uint32          // Number of free IPs in this range
}

func (e1 *entry) Equal(e2 *entry) bool {
	return e1.Token == e2.Token && e1.Peer == e2.Peer &&
		e1.Tombstone == e2.Tombstone && e1.Version == e2.Version
}

// For compatibility with sort.Interface
type entries []*entry

func (es entries) Len() int           { return len(es) }
func (es entries) Less(i, j int) bool { return es[i].Token < es[j].Token }
func (es entries) Swap(i, j int)      { panic("Should never be swapping entries!") }

func (es entries) String() string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "[")
	for i, entry := range es {
		fmt.Fprintf(&buffer, "%+v", *entry)
		if i+1 < len(es) {
			fmt.Fprintf(&buffer, " ")
		}
	}
	fmt.Fprintf(&buffer, "]")
	return buffer.String()
}

func (es entries) entry(i int) *entry {
	i = i % len(es)
	if i < 0 {
		i += len(es)
	}
	return es[i]
}

func (es *entries) insert(e entry) {
	i := sort.Search(len(*es), func(j int) bool {
		return (*es)[j].Token >= e.Token
	})

	if i < len(*es) && (*es)[i].Token == e.Token {
		panic("Trying to insert an existing token!")
	}

	*es = append(*es, &entry{})
	copy((*es)[i+1:], (*es)[i:])
	(*es)[i] = &e
}

func (es entries) get(token uint32) (*entry, bool) {
	i := sort.Search(len(es), func(j int) bool {
		return es[j].Token >= token
	})

	if i < len(es) && es[i].Token == token {
		return es[i], true
	}

	return nil, false
}

// Is token between entries at i and j?
// NB i and j can overflow and will wrap
// NBB if entries[i].token == token, this will return true
func (es entries) between(token uint32, i, j int) bool {
	utils.Assert(i < j, "Start and end must be in order")

	first := es.entry(i)
	second := es.entry(j)

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

// filteredEntries returns the entires minus tombstones
func (es entries) filteredEntries() entries {
	var result entries
	for _, entry := range es {
		if entry.Tombstone == 0 {
			result = append(result, entry)
		}
	}
	return result
}
