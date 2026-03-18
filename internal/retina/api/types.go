package api

import "github.com/dioptra-io/retina-commons/api/v1"

// SequencedForwardingInfoElement adds the sequence number to the FIE.
type SequencedForwardingInfoElement struct {
	api.ForwardingInfoElement
	SequenceNumber uint64 `json:"sequence_number"`
}
