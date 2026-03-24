package api

import "github.com/dioptra-io/retina-commons/api/v1"

// SequencedFIE wraps a ForwardingInfoElement with a sequence number
// for ordered delivery to HTTP clients.
type SequencedFIE struct {
	api.ForwardingInfoElement
	SequenceNumber uint64 `json:"sequence_number"`
}
