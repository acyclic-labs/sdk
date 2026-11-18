use super::GuestState;

pub use acyclic_actors_guest_wire::GuestFuture;

/// Lifecycle context with the admitted Actors socket host.
pub type GuestContext<Bindings> =
    acyclic_actors_guest_wire::GuestContext<Bindings, dyn super::SocketHost>;

/// Builder for the admitted lifecycle context.
pub type GuestContextBuilder<Bindings> =
    acyclic_actors_guest_wire::GuestContextBuilder<Bindings, dyn super::SocketHost>;

/// Typed actor authoring surface with the runtime state and socket host.
pub type ActorDefinition<State, Bindings> = acyclic_actors_guest_wire::ActorDefinition<
    State,
    Bindings,
    GuestState<State>,
    dyn super::SocketHost,
>;

/// Preserve the inferred actor type without introducing an execution runtime.
pub const fn define_actor<State, Bindings>(
    definition: ActorDefinition<State, Bindings>,
) -> ActorDefinition<State, Bindings> {
    definition
}
