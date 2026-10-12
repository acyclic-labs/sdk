# Cloud Worker lifecycle fixtures

The recorded Cloud commit and source assertions are in golden.json. These are normalized protobuf JSON examples from Cloud’s probe and handler tests, not a new schema or a live qualification. Worker publication uses an existing account-owned actors-code bucket. Workers omit the actor export; create/invoke/delete are canonical Actors operations. The Edge forwarding fixture preserves status, body and repeated response headers. Refresh these fixtures with the corresponding Cloud SDK pin update.
