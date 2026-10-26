# Deployment secret selections

Actor and Worker deployments carry an ordered `secrets` list of `{name,
reference}` declarations. Namespace configuration updates carry the same list.
The list is a full replacement: an empty list selects nothing. Bindings and
secrets share one environment-name namespace; duplicates and collisions are
rejected by the canonical Actors admission function.

Manifest lowering resolves only names explicitly listed in `entry.secrets`
against `environment.secrets`. Declaring a secret in an environment grants no
entry access to it. Both runtime kinds use this same lowering function.

References use the bounded `secrets:/scope/name` grammar. They are addresses,
never plaintext, versions, bearer tokens or unwrap credentials. Resolution and
authorization belong to the deployment host; SDK lowering never reads a value.

## Mixed rollout

`SecretSelection` uses new message fields 1 (`name`) and 2 (`reference`).
DeploymentRequest appends field 12; UpdateNamespaceRequest appends field 7.
Existing field numbers, wire kinds and meanings are unchanged. Writers never
encode selections as bindings, reuse predecessor fields or send credential
values. Empty lists emit no new fields.

The retained predecessor descriptor was captured from SDK main `b7333953cb`.
Executable tests decode new messages with that actual reader and verify that
every known predecessor field is unchanged. They also decode predecessor
messages with the new reader and require empty selections.

Unknown-field compatibility does not make an old deployment host resolve
secrets: it ignores the new fields. Deploy hosts that support selections before
enabling nonempty-selection writes. A host must authorize and resolve every
selected reference before accepting a deployment; it must reject an unsupported
selection rather than acknowledge a deployment with missing environment values.
There is no fallback to environment inheritance or plaintext bindings.
