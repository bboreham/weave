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
  - it will continue to ask for space until it receives some, or its
    copy of the ring tells it all peers are full.

### The Ring CRDT

We use a Convergent Replicated Data Type - a CRDT - so that peers can
make changes concurrently and communicate them without central
coordination. To achieve this, we arrange that peers only make changes
to the data structure in ranges that they own (except under
administrator command - see later).

The data structure is a set of tokens containing the name of the
owning peer. Each token is placed at the start address of a range, and
the set is kept ordered so each range goes from one token to the
next. Ranges wrap, so the 'next' token after the last one is the first
token.

When a peer leaves the network, we mark its tokens with a "tombstone"
flag. Tombstone tokens are ignored when considering ownership.

In more detail:
- Each token is a tuple {peer name, version, tombstone flag}, placed
  at an IP address.
- Peer names are taken from Weave: they are unique and survive across restarts.
- Tokens can only be updated by the owning peer, and when this is done
  the version is incremented
- The ring data structure is always gossiped in its entirety
- The merge operation when a peer receives a ring is:
  - Tokens with unique addresses are just copied into the combined ring
  - For tokens at the same address, pick the one with the highest
    version number
- The data gossiped about the ring also includes the amount of free
  space in each range: this is not essential but it improves the
  selection of which peer to ask for space.
- When peers are asked for space, there are four scenarios:
  1. We have an empty range; we can change the peer associated with
     the token at the beginning of the range, increment the version and
     gossip that
  2. We have a range which can be subdivided by a single token to form
     a free range.  We insert said token, mapping to the peer requesting
     space and gossip that.
  3. We have a 'hole' in the middle of a range; an empty range can be
     created by inserting two tokens, one at the beginning of the hole
     mapping to the peer requesting the space, and one at the end of the
     hole mapping to us.
  4. We have no space.

## Initialisation

Peers are told the the address space from which all allocations are
made when starting up.  Each peer must be given the same space.

At start-up, nobody owns any address range.  We deal with concurrent
start-up through a process of leader election.  In essence, the peer
with the highest id claims the entire space for itself, and then
other peers can begin to request ranges from it.  An election is
triggered by some peer being asked to allocate or claim an address.

If a peer elects itself as leader, then it can respond to the request
directly.

However, if the election is won by some different peer, then the peer
that has the request must wait until the leader takes control before
it can request space.

The peer that triggered the election sends a message to the peer it
has elected.  That peer then re-runs the election, to avoid races
where further peers have joined the group and come to a different
conclusion.

Failures:
- two peers that haven't communicated with each other yet can each
  decide to be leader
  -> this is a fundamental problem: if you don't know about someone
     else then you cannot make allowances for them.
- prospective leader dies before sending map
  -> This failure will be detected by the underlying Weave peer
     topology. The peer requiring space will re-try, re-running the
     leadership election across all connected peers.

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


## Data Structures

### Allocator

Allocator runs as a single-threaded Actor, so no locks are used around
data structures.

We need to be able to release any allocations when a container dies, so
Allocator retains a list of those, in a map `owned` indexed by container ID.

When we run out of free addresses we ask another peer to donate space
and wait for it to get back to us, so we have a list of outstanding
'getfor' requests.  There is also a list recording pending claims of
specific addresses; currently this is only needed until we hear of
some ownership on the ring. These are implemented via a common
'operation' interface, although the slightly different semantics
requires us to hold them separately.

Conceptually, Allocator is separate from the hosting Weave process and
its link to the outside world is via its `gossip` and `leadership`
interfaces.

## Limitations

Nothing is persisted. If one peer restarts it should receive gossip
from other peers showing what it used to own, and in that case it can
recover quite well. If, however, there are no peers left alive, or
this peer cannot establish network communication with those that are,
then it cannot recover.
