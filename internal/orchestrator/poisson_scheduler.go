package orchestrator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator/structures"
)

const (
	FIEHistorySize = 6 // This is the total number of elements kept in FIE history.
)

// PoissonScheduler implements the responsible probing and the poisson
// scheduling based on the demo implementation
// [here](https://gist.github.com/ubombar/b09929674d19e6440ad0310cf43568e7).
type PoissonScheduler struct {
	// Config variables
	LearningRate    float64
	MinIssuanceRate float64
	MaxIssuanceRate float64

	logger   *slog.Logger
	eventBus *structures.RingBuffer[SSEEvent]

	ipImpactRecords map[string]*poissonSchedulerRPImpactRecord //nolint:unused // will be used in the responsible probing implementation

	slots      []map[uint64]*poissonSchedulerEntry
	carry      poissonSchedulerEntry
	numSlots   uint64
	slotPeriod time.Duration

	cycleCounter uint64
	slotCounter  uint64
	// startTime denotes the time this wheel started, all the slot and cycle
	// calculations are done using this time.
	startTime time.Time

	fieChan chan *api.ForwardingInfoElement
	pdSet   map[uint64]*api.ProbingDirective
	nodeSet map[uint64]*poissonSchedulerNode

	rand *rand.Rand
}

var _ Scheduler = (*PoissonScheduler)(nil)

// readPDMap reads the PDs from the given jsonl file and returns as a map.
func readPDMap(filepath string) (map[uint64]*api.ProbingDirective, error) {
	f, err := os.Open(filepath) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	results := make(map[uint64]*api.ProbingDirective)
	// bufio has a 64Kb line limit which a single PD is not expected to reach.
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var obj api.ProbingDirective
		if err := json.Unmarshal(scanner.Bytes(), &obj); err != nil {
			return nil, fmt.Errorf("cannot unmarshal line: %w", err)
		}
		results[obj.ProbingDirectiveID] = &obj
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return results, nil
}

// validatePoissonSchedulerConfig checks the scheduler configuration and
// returns an error describing the first invalid parameter found.
func validatePoissonSchedulerConfig(
	wheelSpan,
	slotPeriod time.Duration,
	startingIssuanceRate,
	learningRate,
	minIssuanceRate,
	maxIssuanceRate float64) error {
	if slotPeriod.Milliseconds() <= 0 {
		return fmt.Errorf("slot period cannot be smaller than a millisecond: %v", slotPeriod)
	}
	if wheelSpan < slotPeriod {
		return fmt.Errorf("wheel span (%v) cannot be smaller than the slot period (%v)", wheelSpan, slotPeriod)
	}
	if minIssuanceRate <= 0.0 {
		return fmt.Errorf("minIssuanceRate cannot be zero or less: %v", minIssuanceRate)
	}
	if maxIssuanceRate > 6.0 {
		return fmt.Errorf("maxIssuanceRate cannot be more than 5 times a second: %v", maxIssuanceRate)
	}
	if maxIssuanceRate < minIssuanceRate {
		return fmt.Errorf("maxIssuanceRate (%v) cannot be smaller than minIssuanceRate (%v)", maxIssuanceRate, minIssuanceRate)
	}
	if startingIssuanceRate < minIssuanceRate || startingIssuanceRate > maxIssuanceRate {
		return fmt.Errorf("startingIssuanceRate is not within the range: %v", startingIssuanceRate)
	}
	if learningRate < 0 || learningRate >= 1 {
		return fmt.Errorf("learningRate must be in [0, 1): %v", learningRate)
	}
	return nil
}

func NewPoissonScheduler(
	seed uint64,
	pdFile string,
	wheelSpan,
	slotPeriod time.Duration,
	fieChanSize int,
	startingIssuanceRate,
	learningRate,
	minIssuanceRate,
	maxIssuanceRate float64,
	logger *slog.Logger,
	eventBus EventBus,
) (*PoissonScheduler, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if err := validatePoissonSchedulerConfig(
		wheelSpan, slotPeriod,
		startingIssuanceRate, learningRate,
		minIssuanceRate, maxIssuanceRate); err != nil {
		return nil, err
	}

	pdSet, err := readPDMap(pdFile)
	if err != nil {
		return nil, fmt.Errorf("cannot read from file: %w", err)
	}
	if len(pdSet) == 0 {
		return nil, fmt.Errorf("invalid arguments: pds length cannot be zero")
	}
	logger.Info("Scheduler loaded directives",
		slog.Int("count", len(pdSet)),
		slog.String("file", pdFile))
	n := uint64(len(pdSet))
	numSlots := uint64(math.Ceil(wheelSpan.Seconds() / slotPeriod.Seconds()))
	slots := make([]map[uint64]*poissonSchedulerEntry, numSlots)
	for i := range numSlots {
		slots[i] = make(map[uint64]*poissonSchedulerEntry, 1)
	}
	nodeSet := make(map[uint64]*poissonSchedulerNode, n)

	sched := &PoissonScheduler{
		logger:          logger,
		eventBus:        eventBus,
		LearningRate:    learningRate,
		MinIssuanceRate: minIssuanceRate,
		MaxIssuanceRate: maxIssuanceRate,
		ipImpactRecords: make(map[string]*poissonSchedulerRPImpactRecord),
		slots:           slots,
		carry: poissonSchedulerEntry{
			head: nil,
			tail: nil,
			size: 0,
		},
		numSlots:     numSlots,
		slotPeriod:   slotPeriod,
		cycleCounter: 0,
		slotCounter:  0,
		fieChan:      make(chan *api.ForwardingInfoElement, fieChanSize),
		pdSet:        pdSet,
		nodeSet:      nodeSet,
		rand:         rand.New(rand.NewSource(int64(seed))), //nolint:gosec // G404: math/rand is fine, not used for security
	}

	sched.initNodes(startingIssuanceRate)
	return sched, nil
}

// NextPD is the actual call made for getting the latest PD. The caller is
// trusted to make as many calls as possible to sustain the issuance rate.
//
// When issue is called it checks if we are still in the same time slot, if we
// are in the same slot then it issues the entries in the slot, if not then it
// collects all the previous entries into the carry, push it to the current slot
// and then continue normally.
func (w *PoissonScheduler) NextPD() (*api.ProbingDirective, error) {
	if w.startTime.IsZero() {
		w.startTime = time.Now()
	}

	// The reason we can do this is because the expected number of FIEs are
	// smaller than the sent PDs. We want to consume all the incoming FIE
	// updates before issuing any PDs.
	exit := false
	for !exit {
		select {
		case fie := <-w.fieChan:
			w.updateFIE(fie)
		default:
			exit = true
		}
	}

	var n *poissonSchedulerNode
	for {
		prevCycle, prevSlot := w.cycleCounter, w.slotCounter
		w.updateCounters()
		curCycle, curSlot := w.cycleCounter, w.slotCounter

		// Check if we are not in the same cycle and slot.
		if prevCycle != curCycle || prevSlot != curSlot {
			// We need to cary all the past entries.
			for iCycle, iSlot := range w.pairsBetween(prevCycle, prevSlot, curCycle, curSlot) {
				w.carryOver(iCycle, iSlot)
			}
		}

		// Finish the carry first.
		if curNode := w.carry.pop(); curNode != nil {
			n = curNode
			break
		}

		// If there are no elements in the carry, continue normal issuance.
		// Check if there are elements in the current slot, if not busy wait.
		if curEntry, ok := w.slots[curSlot][curCycle]; ok {
			if curNode := curEntry.pop(); curNode != nil {
				n = curNode
				break
			}
		}

		// If there are no elements to consume the best bet is to do a busywait and
		// go back to the beginning. The outer for loop has no end meaning this
		// function would block until an issuance time comes for an element. If
		// there are no elements then it would block forever, which should never
		// happen.
		nextSlotTime := w.nextSlotTime(curCycle, curSlot)
		// We do a bustwait because sleep is not realiable for short waiting times.
		for time.Now().Before(nextSlotTime) {
		}
	}

	// When we have the node, we need to re-schedule it so there will always be
	// an element in the time-wheel.
	n.issueCounter++
	w.reschedule(n)
	return w.pdSet[n.pdid], nil
}

func (w *PoissonScheduler) UpdateFromFIE(fie *api.ForwardingInfoElement) error {
	w.fieChan <- fie
	return nil
}

// initNodes creates a scheduler node per PD, all scheduled into the first
// slot of the wheel. Scheduling everything at slot (0,0) is not ideal and
// should be spread out in a future iteration.
func (w *PoissonScheduler) initNodes(startingIssuanceRate float64) {
	for _, pd := range w.pdSet {
		node := &poissonSchedulerNode{
			pdid:           pd.ProbingDirectiveID,
			issueCounter:   1,
			issuancePeriod: structures.AtomicFloat64{},
			scheduler:      w,
		}
		node.issuancePeriod.Store(1.0 / startingIssuanceRate)
		w.nodeSet[pd.ProbingDirectiveID] = node

		if e, ok := w.slots[0][0]; ok {
			e.push(node)
		} else {
			w.slots[0][0] = &poissonSchedulerEntry{head: node, tail: node, size: 1}
		}
	}
}

// updateCounters computes the time passed since the startTime and updates the
// cycleCounter and slotCounter
func (w *PoissonScheduler) updateCounters() {
	passedMs := uint64(time.Since(w.startTime).Milliseconds()) //nolint:gosec // G115: elapsed time is never negative
	slotMs := uint64(w.slotPeriod.Milliseconds())              //nolint:gosec // G115: elapsed time is never negative
	totalSlots := passedMs / slotMs
	w.cycleCounter = totalSlots / w.numSlots
	w.slotCounter = totalSlots % w.numSlots
}

func (w *PoissonScheduler) nextSlotTime(curCycle, curSlot uint64) time.Time {
	totalSlots := int64(w.numSlots*curCycle + curSlot + 1) //nolint:gosec // G115: elapsed time is never negative
	return w.startTime.Add(w.slotPeriod * time.Duration(totalSlots))
}

// updateFIE adds the hash of the FIE to the node. This happens in the same
// goroutine.
func (w *PoissonScheduler) updateFIE(fie *api.ForwardingInfoElement) {
	if n, ok := w.nodeSet[fie.ProbingDirectiveID]; ok {
		n.insertToFIEHistory(fie)
		n.updateIssuanceRate()

		// TODO: This is the place where responsible probabing recordings should
		// take place. However time constraints doesn't allow me to implement this
		// right now, thus pushing this into future.
		if fie.NearInfo != nil {
			n.lastHitNearAddress = fie.NearInfo.ReplyAddress
		}
		if fie.FarInfo != nil {
			n.lastHitFarAddress = fie.FarInfo.ReplyAddress
		}
	}
}

// reschedule does a new sampling from the exponential distribution and adds the
// node back again into the scheduler.
func (w *PoissonScheduler) reschedule(n *poissonSchedulerNode) {
	u := w.rand.Float64()
	if u == 0 {
		u = 1e-10 // prevent log(0)
	}

	// Load atomically to prevent race.
	issuancePeriod := n.issuancePeriod.Load()
	delaySeconds := -math.Log(u) * issuancePeriod

	slotOffset := uint64(math.Ceil(delaySeconds / w.slotPeriod.Seconds()))
	if slotOffset == 0 {
		slotOffset = 1
	}

	// Convert the slot offset into a cycle, slot pair and insert it.
	curCycle, curSlot := w.cycleCounter, w.slotCounter
	schedSlot := (curSlot + slotOffset) % w.numSlots
	schedCycle := curCycle + (curSlot+slotOffset)/w.numSlots

	if e, ok := w.slots[schedSlot][schedCycle]; ok {
		e.push(n)
	} else {
		w.slots[schedSlot][schedCycle] = &poissonSchedulerEntry{
			head: n,
			tail: n,
			size: 1,
		}
	}
}

// pairsBetween returns an iterator to iterate between the given (cycle, slot)
// pairs. Note that the last cycle and slot pair is excluded from the iterator.
func (w *PoissonScheduler) pairsBetween(minCycle, minSlot, maxCycle, maxSlot uint64) func(func(uint64, uint64) bool) {
	return func(yield func(uint64, uint64) bool) {
		for cycle := minCycle; cycle <= maxCycle; cycle++ {
			startSlot, endSlot := uint64(0), w.numSlots-1
			if cycle == minCycle {
				startSlot = minSlot
			}
			if cycle == maxCycle {
				endSlot = maxSlot
			}

			for slot := startSlot; slot <= endSlot; slot++ {
				// the cycle, slot pair is ready to be served. But check so that
				// the maxCycle, maxSlot pair is excluded.
				if cycle == maxCycle && slot == maxSlot {
					return
				}
				if !yield(cycle, slot) {
					return
				}
			}
		}
	}
}

// carry will get the elements in the slot and puts them into the carry. If it
// is empty this is a noop.
func (w *PoissonScheduler) carryOver(cycle, slot uint64) {
	slotEntry, ok := w.slots[slot][cycle]
	// not ok means there are no elements in this cycle, slot pair. NOOP.
	if !ok {
		return
	}
	if slotEntry.size == 0 {
		delete(w.slots[slot], cycle)
		return
	}

	// move items of the current slot to the carry O(1).
	slotEntry.move(&w.carry)
	delete(w.slots[slot], cycle)
}

// helper structs
type poissonSchedulerEntry struct {
	head *poissonSchedulerNode
	tail *poissonSchedulerNode
	size int
}

// push adds a new node to the entry, it is appended to the end of the linked
// list.
func (e *poissonSchedulerEntry) push(n *poissonSchedulerNode) {
	n.next = nil
	if e.tail == nil {
		e.head = n
		e.tail = n
	} else {
		e.tail.next = n
		e.tail = n
	}
	e.size += 1
}

// pop returns an element from the beginning of the linked list.
func (e *poissonSchedulerEntry) pop() *poissonSchedulerNode {
	if e.head == nil {
		return nil
	}
	r := e.head
	e.head = e.head.next
	e.size -= 1
	return r
}

// move moves the elements into the given entry struct.
func (e *poissonSchedulerEntry) move(a *poissonSchedulerEntry) {
	if e.size == 0 {
		return
	}
	if a.size == 0 {
		a.head = e.head
		a.tail = e.tail
		a.size = e.size
	} else {
		a.tail.next = e.head
		a.tail = e.tail
		a.size += e.size
	}
	e.head = nil
	e.tail = nil
	e.size = 0
}

type poissonSchedulerNode struct {
	scheduler *PoissonScheduler
	pdid      uint64
	next      *poissonSchedulerNode
	// issueCounter is incremented every time this PD is issued.
	issueCounter int
	// issuancePeriod denotes the mean issuance period of this PD. For our purposes,
	// the sampling is done using exponential sampling, meaning PDs follow a
	// Poisson process.
	issuancePeriod structures.AtomicFloat64
	// this is the hsitory of the FIE for that PD.
	fieHistory         [FIEHistorySize]string
	fieHistoryPointer  int
	lastHitNearAddress net.IP
	lastHitFarAddress  net.IP
}

// updateIssuanceRate recomputes the issuance period of the PD connected to
// this node. The current simple implementation is to lengthen the period of
// (i.e. slow down) very stable PDs. Adjustments only start once a full window
// of FIEHistorySize observations has been collected.
func (n *poissonSchedulerNode) updateIssuanceRate() {
	unique, total := n.numUniqueFIEs()
	if total < FIEHistorySize {
		// Not enough observations yet to judge stability.
		return
	}

	oldPeriod := n.issuancePeriod.Load()
	var newPeriod float64

	if unique == 1 {
		// Stable window: slow down.
		newPeriod = oldPeriod * (1 + n.scheduler.LearningRate)
	} else {
		// Unstable window: speed up.
		newPeriod = oldPeriod / (1 + n.scheduler.LearningRate)
	}

	// Clamp to [MinIssuanceRate, MaxIssuanceRate].
	if 1.0/newPeriod < n.scheduler.MinIssuanceRate {
		newPeriod = 1.0 / n.scheduler.MinIssuanceRate
	}
	if 1.0/newPeriod > n.scheduler.MaxIssuanceRate {
		newPeriod = 1.0 / n.scheduler.MaxIssuanceRate
	}

	if newPeriod == oldPeriod {
		// Already at the clamp boundary; nothing changed, no event.
		return
	}

	n.issuancePeriod.Store(newPeriod)
	n.scheduler.eventBus.Push(&SSEEvent{
		Type:      SSEEventRateAdjusted,
		Timestamp: time.Now(),
		Data: &RateAdjustedData{
			ProbingDirectiveID: n.pdid,
			PreviousRate:       1.0 / oldPeriod,
			CurrentRate:        1.0 / newPeriod,
		},
	})
}

func (n *poissonSchedulerNode) insertToFIEHistory(fie *api.ForwardingInfoElement) {
	n.fieHistory[n.fieHistoryPointer] = hashFIE(fie)
	n.fieHistoryPointer = (n.fieHistoryPointer + 1) % FIEHistorySize
}

// numUniqueFIEs returns the number of unique FIE hashes in the history and the
// total number of observed (non-empty) entries. Uniqueness is only meaningful
// relative to total: e.g. unique==1 with total==6 means a full, stable history,
// while unique==1 with total==1 just means we've only seen one FIE so far.
func (n *poissonSchedulerNode) numUniqueFIEs() (unique, total int) {
	seen := make(map[string]struct{}, FIEHistorySize)
	for _, e := range n.fieHistory {
		if e != "" {
			seen[e] = struct{}{}
			total++
		}
	}
	return len(seen), total
}

// pdState holds the scheduling state for a single ProbingDirective, including
// the last observed near and far addresses and the current issuance probability.
type poissonSchedulerRPState struct { //nolint:unused // will be used in the responsible probing implementation
	lastHitNearAddress net.IP
	lastHitFarAddress  net.IP
	issuanceProb       float64
	directive          *api.ProbingDirective
}

// impactRecord stores the current impact state for a single address.
type poissonSchedulerRPImpactRecord struct { //nolint:unused // will be used in the responsible probing implementation
	// pds is the set of ProbingDirective IDs currently impacting this address.
	pds map[uint64]*poissonSchedulerRPState //nolint:unused
}

// hashFIE converts the given fie into a hash string representation. This is
// used for the FIE history uniqueness.
func hashFIE(fie *api.ForwardingInfoElement) string {
	nearStr, farStr := "", ""
	if fie.NearInfo != nil {
		nearStr = fie.NearInfo.ReplyAddress.String()
	}
	if fie.FarInfo != nil {
		farStr = fie.FarInfo.ReplyAddress.String()
	}
	return fmt.Sprintf("%v-%v", nearStr, farStr)
}
