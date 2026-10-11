#!/usr/bin/env bash
# Regenerate the gRPC API. One proto per output tree: events.proto is the
# client-facing surface (pkg/grpcapi), peer.proto is the inter-node plane
# (internal/grpcapi). Always run this script rather than buf generate directly:
# a bare run applies one template to every proto and would drop peer.pb.go back
# into pkg/grpcapi (the rm below also clears such a leftover).
set -euo pipefail
cd "$(dirname "$0")/../proto"
rm -f ../pkg/grpcapi/pushupes/v1/peer.pb.go ../pkg/grpcapi/pushupes/v1/peer_grpc.pb.go
./buf generate --path pushupes/v1/events.proto
./buf generate --template buf.gen.peer.yaml --path pushupes/v1/peer.proto
