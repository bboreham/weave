See the [requirements](https://github.com/zettio/weave/wiki/IP-allocation-requirements).

At its highest level, the idea is that we start with a certain IP
address space, a subnet specified by a CIDR (e.g. "10.3.0.0/16"),
known to all peers, and divide it up between the peers. This allows
peers to allocate and free individual IPs locally, until they run out.

We use a CRDT to represent shared knowledge about the subnet,
transmitted over the Weave Gossip mechanism, together with
point-to-point messages for one peer to request more space from
another.

The allocator running at each peer also has an http interface which
the container infrastructure (e.g. the Weave script, or a Docker
plugin) uses to request IP addresses.

![Schematic of IP allocation in operation](https://docs.google.com/drawings/d/1-EUIRKYxwfKTpBJ7v_LMcdvSpodIMSz4lT3wgEfWKl4/pub?w=701&h=310)

## Commands

The commands supported by the allocator are:

- Allocate: request one IP address for a container
- Delete records for one container: all IP addresses allocated to that
  container are freed.
- Claim: request a specific IP address for a container (e.g. because
  it is already using that address)
- Free: return an IP address that is currently allocated

## Definitions

1. Allocations. We use the word 'allocation' to refer to a specific
   IP address being assigned, e.g. so it can be given to a container.

2. Range. Most of the time, instead of dealing with individual IP
   addresses, we operate on them in contiguous groups, for which we
   use the word "range".  A range has a start address and a length.

3. Subnet. The address space from which all ranges are
   allocated. This is configured at start-up and cannot be changed
   during the run-time of the system.

### The Ring

We consider the subnet as a ring, and place tokens at the start of
each range owned by a peer.  The peer owns every address from the
start token up to but not including the next token which denotes
another owned range. Ranges wrap around the end of the subnet.

This ring is a CRDT.  Peers only ever make changes in ranges that they
own (except under administrator command - see later). This makes the
data structure inherently convergent.

In more detail:
- Each token is a tuple {peer name, version, tombstone flag}, placed at an IP address.
- Peer names are taken from Weave: they are unique and survive across restarts.
- A peer owns the ranges indicated by the tokens it owns.
- A token can only be inserted by the peer owning the range it is inserted into.
- Entries in the map can only be updated by the owning peer, and when
  this is done the version is incremented
- The map is always gossiped in its entirety
- The merge operation when a peer receives a map is:
  - Disjoint tokens are just copied into the combined map
  - For entries with the same token, pick the highest version number
- If a token maps to a tombstone, this indicates that the previous
  owning peer that has left the network.
  - For the purpose of range ownership, tombstones are ignored - ie
    ranges extend past tombstones.
  - Tombstones are only inserted by an administrative action (see below)
  - Tombstones expire and are removed from the ring after two weeks.
- The data gossiped about the ring also includes the amount of free
  space in each range: this is not essential but it improves the
  selection of which peer to ask for space.

### The allocator

- The allocator can allocate freely to containers on your machine from ranges you own
  - This data does not need to be persisted (assumed to have the same failure domain)
- If the allocator runs out of space (all owned ranges are full), it
  will ask another peer for space
  - we pick a peer to ask at random, weighted by the amount of space
    owned by each peer
  - if the target peer decides to give up space, it unicasts a message
    back to the asker with the newly-updated ring.
  - we will continue to ask for space until we receive some via the
    gossip mechanism, or our copy of the ring tells us all peers are
    full.

- When peers are asked for space, there are 4 scenarios:
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

Peers are told the subnet - the IP range from which all allocations
are made - when starting up.  Each peer must be given the same range.

At start-up, nobody owns any address range.  We deal with concurrent
start-up through a process of leader election.  In essence, the peer
with the highest id claims the entire subnet for itself, and then
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


## Future work

Currently there is no fall-back range if you don't specify one on the
command-line; it would be better to pick from a set of possibilities
(checking the range is currently unused before we start using it).

How to use IPAM for WeaveDNS?  It needs its own special subnet.

How should we add support for subnets in general?

We get a bit of noise in the weave logs from containers going down,
now that the weave script is calling ethtool and curl via containers.

If we hear that a container has died we should knock it out of pending?

Interrogate Docker to check container exists and is alive at the point we assign an IP

To lessen the chance of simultaneous election of two leaders at
start-up, peers could wait for a bit to see if more peers arrive
before running an election.  Also, we could look at the command-line
parameters supplied to Weave, and wait longer in the case that we have
been asked to connect to someone specific.

[It would be good to move PeerName out of package router into a more
central package so both ipam and router can depend on it.]

There is no specific logic to time-out requests such as "give me some
space": the logic will be re-run and the request re-sent next time
something else happens (e.g. it receives another request, or some
periodic gossip). This means we may send out requests more frequently
than required, but this is innoccuous and it keeps things simple.  It
is possible for nothing to happen, e.g. if everyone else has
disconnected.  We could have a timer to re-consider things.

The operation to Split() one space into two, to give space to another
Peer, is conceptually simple but the implementation is fiddly to
maintain the various lists and limits within MutableSpace. Perhaps a
different free-list implementation would make this easier.
