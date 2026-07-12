package orchestrator

import "github.com/dioptra-io/retina-commons/api/v1"

type Scheduler interface {
	// NextPD retrieves the next issuable PD. It blocks if the time is not come
	// yet.
	NextPD() (*api.ProbingDirective, error)

	// UpdateFromFIE updates the PD by the yielded FIE.
	UpdateFromFIE(fie *api.ForwardingInfoElement) error
}
