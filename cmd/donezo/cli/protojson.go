package cli

import "google.golang.org/protobuf/encoding/protojson"

var unmarshalOptions = protojson.UnmarshalOptions{
	DiscardUnknown: true,
}

var marshalOptions = protojson.MarshalOptions{
	EmitUnpopulated: true,
}
