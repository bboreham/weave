See the [requirements](https://github.com/zettio/weave/wiki/IP-allocation-requirements).

At its highest level, the idea is that we start with a certain IP
address space, known to all peers, and divide it up between the
peers. This allows peers to allocate and free individual IPs locally
until they run out.

We use a CRDT to represent shared knowledge about the space,
transmitted over the Weave Gossip mechanism, together with
point-to-point messages for one peer to request more space from
another.

The allocator running at each peer also has an http interface which
the container infrastructure (e.g. the Weave script, or a Docker
plugin) uses to request IP addresses.

![Schematic of IP allocation in operation](https://docs.google.com/drawings/d/1-EUIRKYxwfKTpBJ7v_LMcdvSpodIMSz4lT3wgEfWKl4/pub?w=701&h=310)

## Commands

The commands supported by the allocator via the http interface are:

- Allocate: request one IP address for a container
- Free: return an IP address that is currently allocated
- Claim: request a specific IP address for a container (e.g. because
  it is already using that address)

The allocator also watches via the Docker event mechanism: if a
container dies then all IP addresses allocated to that container are
freed.

## Definitions

1. Allocations. We use the word 'allocation' to refer to a specific
   IP address being assigned, e.g. so it can be given to a container.

2. Range. Most of the time, instead of dealing with individual IP
   addresses, we operate on them in contiguous groups, for which we
   use the word "range".

3. Ring. We consider the address space as a ring, so ranges wrap
   around from the highest address to the lowest address.

4. Peer. A Peer is a node on the Weave network. It can own zero or
   more ranges.

### The Allocation Process

When a peer owns some range(s), it can allocate freely from within
those ranges to containers on the same machine. If it runs out of
space (all owned ranges are full), it will ask another peer for space:
  - it picks a peer to ask at random, weighted by the amount of space
    owned by each peer
    - if the target peer decides to give up space, it unicasts a message
      back to the asker with the newly-updated ring.
    - if the target peer has no space, it unicasts a message back to the
      asker with its current copy of the ring,on the basis tha the
      requestor must have acted on out-of-date information.
  - it will continue to ask for space until it receives some, or its
    copy of the ring tells it all peers are full.

### Claiming an address

If a Weave process is restarted, in most cases it will hear from
another peer which ranges it used to own, but it needs to know which
individual IP addresses are assigned to containers in order to avoid
giving the same address out in subsequent allocation requests. The
weave script invokes the `claim` command in `populate_ipam` to do
this.

When the Allocator is told to claim an address, there are four
possibilities:
  - the address is outside of the space managed by this Allocator, in
    which case we ignore the request.
  - the address is owned by this peer, in which case we record the
    address as being assigned to a particular container and return
    success.
  - the address is owned by a different peer, in which case we return
    failure.
  - we have not yet heard of any address ranges being owned by anyone,
    in which case we wait until we do hear.

This approach fails if the peer does not hear from another peer about
the ranges it used to own, e.g. if all peers in a network partition
are restarted at once.

### The Ring CRDT

We use a Convergent Replicated Data Type - a CRDT - so that peers can
make changes concurrently and communicate them without central
coordination. To achieve this, we arrange that peers only make changes
to the data structure in ranges that they own (except under
administrator command - see later).

The data structure is a set of tokens, each containing the name of an
owning peer. A peer can own many ranges. Each token is placed at the
start address of a range, and the set is kept ordered so each range
goes from one token to the next. Each range on the ring includes the
start address but does not include the end address (which is the start
of the next range).  Ranges wrap, so the 'next' token after the last
one is the first token.

![Tokens on the Ring](https://docs.google.com/drawings/d/1hp--q2vmxbBAnPjhza4Kqjr1ugrw2iS1M1GerhH-IKY/pub?w=960&h=288)

When a peer leaves the network, we mark its tokens with a "tombstone"
flag. Tombstone tokens are ignored when considering ownership.

In more detail:
- Each token is a tuple {peer name, version, tombstone flag}, placed
  at an IP address.
- Peer names are taken from Weave: they are unique and survive across restarts.
- The contents of a token can only be updated by the owning peer, and
  when this is done the version is incremented
- The ring data structure is always gossiped in its entirety
- The merge operation when a peer receives a ring via gossip is:
  - Tokens with unique addresses are just copied into the combined ring
  - For tokens at the same address, pick the one with the highest
    version number
- The data gossiped about the ring also includes the amount of free
  space in each range: this is not essential but it improves the
  selection of which peer to ask for space.
- When a peer is asked for space, there are four scenarios:
  1. It has an empty range; it can change the peer associated with
     the token at the beginning of the range and increment the version.
  2. It has a range which can be subdivided by a single token to form
     a free range.  It inserts said token, owned by the peer requesting
     space.
  3. It has a 'hole' in the middle of a range; an empty range can be
     created by inserting two tokens, one at the beginning of the hole
     owned by the peer requesting the space, and one at the end of the
     hole owned by the requestee.
  4. It has no space.
- Tombstones are designed to expire after 2 weeks.  Host clocks need
  to be reasonably within sync; if the host clocks differ by more than
  2 weeks, peer deletion will behave inconsistently. Similarly, you
  should not allow network partitions to persist for longer than 2 weeks,
  as you may see previously deleted hosts reappearing.  Nodes gossip
  their current time, and if a receiving host detects more than 1 hour
  of clock skew, the gossip will be rejected and the connection dropped.

## Initialisation

Peers are told the the address space from which all allocations are
made when starting up.  Each peer must be given the same space.

When a peer starts up, it owns no address ranges. If it joins a
network in which peers already own space, then it can request space
from another peer as above. If nobody in the network owns any space
then we follow a process similar to leader election: one peer claims
the entire space for itself, and then other peers can begin to request
ranges from it.

1. An election is triggered by some peer being asked (via command) to
   allocate or claim an address.
2. That peer looks at all peers it knows about, and picks the one with
   the highest ID.
3. If the one picked is itself, then it inserts a token owned by
   itself into the ring, broadcasts the ring via gossip, then responds
   to the command directly.
4. However, if another peer has the highest ID, the peer that
   triggered the election sends a message to the peer it has picked,
   and waits to hear back. A peer receiving such a message behaves as
   in step 2, i.e. it re-runs the process, possibly choosing yet
   another peer and sending it a message requesting it to take over.

Step 4 is designed to cope with peers simultaneously joining the
network, when information about who has joined has not reached all
peers yet. By sending a message to the peer we /think/ should be in
charge, we ensure that the choice is made using the combination of all
information available to all peers in the network.

If a peer dies just after it has been chosen, then the originating
peer will not hear back from it. This failure will be detected by the
underlying Weave peer topology and the dead peer will be removed from
the set. The originating peer will re-try, re-running the process
across all connected peers.

Sets of peers that don't know about each other, either because the
network is partitioned or because they just haven't connected yet,
will each pick a different leader. This problem seems fundamental.


## Peer shutdown

When a peer leaves (a `weave reset` command), it updates all its own
tokens to be tombstones, then broadcasts the updated ring.  This
causes its space to be inherited by the owner of the previous tokens
on the ring, for each range.

After sending the message, the peer terminates - it does not wait for
any response.

Failures:
- message lost
  - the space will be unusable by other peers because it will still be
    seen as owned.

To cope with the situation where a peer has left or died without
managing to tell its peers, an administrator may go to any other peer
and command that it mark the dead peer's tokens as tombstones (with
`weave rmpeer`).  This information will then be gossipped out to the
network.


## Limitations

Nothing is persisted. If one peer restarts it should receive gossip
from other peers showing what it used to own, and in that case it can
recover quite well. If, however, there are no peers left alive, or
this peer cannot establish network communication with those that are,
then it cannot recover.
