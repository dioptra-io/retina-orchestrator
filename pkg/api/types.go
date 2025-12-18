package settings

type Settings struct {
	GlobalProbingRate uint     `json:"global_probing_rate"`
	AvailableAgentIDs []string `json:"available_agent_ids"`
}
