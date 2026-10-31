// Only the epoch protocol is modeled here. Item contents, renderer and model
// are fixed; a key encodes (source stream, covered prefix). MaxHead bounds the
// model-checker's state space, not the production conversation.
module epochs {
  pure val MaxHead = 4
  pure val Stride = MaxHead + 1
  pure val Agents = Set(0, 1)
  pure val Slots = Set(0, 1)
  pure val Keys = 0.to(2 * Stride - 1)

  type View = { head: int, cursor: int, generation: int, pending: int, summary: int }
  type Proposal = { agent: int, generation: int, key: int }
  type State = {
    views: int -> View,
    jobs: int -> int,
    proposals: int -> Proposal,
    forked: bool,
    common: int,
    crashed: bool
  }
  var s: State

  pure val emptyView = { head: 0, cursor: 0, generation: 0, pending: 0, summary: 0 }
  pure val emptyProposal = { agent: 0, generation: 0, key: 0 }
  pure def through(key: int): int = key % Stride
  pure def source(key: int): int = key / Stride
  def live(a: int): bool = a == 0 or s.forked
  def within(a: int, key: int): bool =
    source(key) == a or (a == 1 and source(key) == 0 and through(key) <= s.common)

  action init = s' = {
    views: Agents.mapBy(_ => emptyView),
    jobs: Keys.mapBy(_ => 0),
    proposals: Slots.mapBy(_ => emptyProposal),
    forked: false,
    common: 0,
    crashed: false
  }

  action append(a: int) = {
    val v = s.views.get(a)
    all {
      live(a),
      v.head < MaxHead,
      s' = { ...s, views: s.views.set(a, { ...v, head: v.head + 1 }) }
    }
  }

  // One conditional commit claims the producer and pins this consumer's cut.
  // Existing keys reuse the original producer or its terminal result.
  action prepare(a: int, cut: int) = {
    val v = s.views.get(a)
    val ancestor = if (a == 1 and cut <= s.common) 0 else a
    val key = ancestor * Stride + cut
    val jobs = if (s.jobs.get(key) == 0) s.jobs.set(key, 1) else s.jobs
    all {
      live(a),
      v.pending == 0,
      v.cursor < cut and cut <= v.head,
      s' = { ...s, jobs: jobs, views: s.views.set(a, { ...v, pending: key }) }
    }
  }

  // A producer always publishes a terminal event, including custody loss.
  // Neither terminal outcome can later be rewritten as the other.
  action complete(key: int, success: bool) = all {
    s.jobs.get(key) == 1,
    s' = { ...s, jobs: s.jobs.set(key, if (success) 2 else 3) }
  }

  action propose(slot: int, a: int) = {
    val v = s.views.get(a)
    all {
      live(a),
      v.pending != 0,
      s.jobs.get(v.pending) == 2,
      s.proposals.get(slot).key == 0,
      s' = { ...s, proposals: s.proposals.set(slot, { agent: a, generation: v.generation, key: v.pending }) }
    }
  }

  // This is the linearization point. A competing swap fences an old proposal.
  action commit(slot: int) = {
    val p = s.proposals.get(slot)
    val v = s.views.get(p.agent)
    val next = { ...v, cursor: through(p.key), generation: v.generation + 1, pending: 0, summary: p.key }
    all {
      p.key != 0,
      p.generation == v.generation,
      p.key == v.pending,
      s.jobs.get(p.key) == 2,
      within(p.agent, p.key),
      through(p.key) <= v.head,
      s' = { ...s, views: s.views.set(p.agent, next), proposals: s.proposals.set(slot, emptyProposal) }
    }
  }

  action discardStale(slot: int) = {
    val p = s.proposals.get(slot)
    all {
      p.key != 0,
      p.generation != s.views.get(p.agent).generation,
      s' = { ...s, proposals: s.proposals.set(slot, emptyProposal) }
    }
  }

  action fork = {
    val parent = s.views.get(0)
    all {
      not(s.forked),
      parent.pending == 0,
      // Fan-out inherits a completed swap, never unfinished producer custody.
      s' = { ...s, forked: true, common: parent.head, views: s.views.set(1, parent) }
    }
  }

  // Proposals are volatile; committed epochs and shared results survive.
  // A crash of a producer is represented separately by complete(key, false).
  action crash = s' = { ...s, proposals: Slots.mapBy(_ => emptyProposal), crashed: true }

  action step = any {
    { nondet a = Agents.oneOf()
      append(a) },
    { nondet a = Agents.oneOf()
      nondet cut = 1.to(MaxHead).oneOf()
      prepare(a, cut) },
    { nondet key = Keys.oneOf()
      nondet success = Bool.oneOf()
      complete(key, success) },
    { nondet a = Agents.oneOf()
      nondet slot = Slots.oneOf()
      propose(slot, a) },
    { nondet slot = Slots.oneOf()
      commit(slot) },
    { nondet slot = Slots.oneOf()
      discardStale(slot) },
    fork,
    crash
  }

  val partition = Agents.forall(a => {
    val v = s.views.get(a)
    val covered = if (v.summary == 0) Set() else 0.to(through(v.summary) - 1)
    val verbatim = v.cursor.to(v.head - 1)
    v.cursor >= 0 and v.cursor <= v.head and
      covered.intersect(verbatim) == Set() and
      covered.union(verbatim) == 0.to(v.head - 1)
  })
  val sharedPrefix = Agents.forall(a => {
    val v = s.views.get(a)
    ((v.summary == 0 and v.cursor == 0) or
      (v.summary != 0 and within(a, v.summary) and through(v.summary) == v.cursor and s.jobs.get(v.summary) == 2)) and
      (v.pending == 0 or (within(a, v.pending) and through(v.pending) <= v.head))
  })
  val safety = partition and sharedPrefix

  // A cursor-only swap loses the item even though its two numeric ranges join.
  // This action is deliberately excluded from the production step relation.
  action omitSummary = s' = {
    ...s, views: s.views.set(0, { ...s.views.get(0), cursor: 1 })
  }
  run missingSummaryNegativeControl = init.then(append(0)).then(omitSummary)
    .expect(not(safety))

  run raceAndCrash = init
    .then(append(0)).then(append(0)).then(prepare(0, 2))
    .then(complete(2, true))
    .then(propose(0, 0)).then(propose(1, 0)).then(commit(0))
    .then(discardStale(1)).then(fork)
    .then(append(0)).then(prepare(0, 3)).then(complete(3, true))
    .then(propose(0, 0)).then(crash)
    .then(propose(0, 0)).then(commit(0))
    .expect(safety and s.views.get(1).cursor == 2 and s.crashed)

  run sharedAncestor = init
    .then(append(0)).then(append(0)).then(prepare(0, 1))
    .then(complete(1, true)).then(propose(0, 0)).then(commit(0)).then(fork)
    .then(prepare(0, 2)).then(prepare(1, 2)).then(complete(2, true))
    .then(propose(0, 0)).then(propose(1, 1)).then(commit(0)).then(commit(1))
    .expect(safety and s.views.get(0).summary == s.views.get(1).summary)

  run producerDeath = init.then(append(0)).then(prepare(0, 1))
    .then(complete(1, false)).expect(s.jobs.get(1) == 3 and s.views.get(0).cursor == 0)
}
