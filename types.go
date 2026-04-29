package junyul

// ClassificationRequest mirrors the REST API schema.
type ClassificationRequest struct {
	Name                    string `json:"name,omitempty"`
	Domain                  string `json:"domain"`
	DecisionAutonomy        string `json:"decision_autonomy"`
	UserImpact              string `json:"user_impact,omitempty"`
	ProcessesPersonalData   bool   `json:"processes_personal_data,omitempty"`
	AiType                  string `json:"ai_type,omitempty"`
	GeneratesContent        bool   `json:"generates_content,omitempty"`
	IndistinguishableOutput bool   `json:"indistinguishable_output,omitempty"`
	AdditionalContext       string `json:"additional_context,omitempty"`
}

type CitedClause struct {
	Type  string `json:"type"`
	Ref   string `json:"ref"`
	Title string `json:"title,omitempty"`
}

type RecommendedAction struct {
	Code                   string `json:"code"`
	Priority               string `json:"priority"`
	Description            string `json:"description,omitempty"`
	Clause                 string `json:"clause,omitempty"`
	EstimatedEffortHours   int    `json:"estimated_effort_hours,omitempty"`
}

type ClassificationResult struct {
	PrimaryCategory      string              `json:"primary_category"`
	SecondaryCategories  []string            `json:"secondary_categories"`
	Confidence           float64             `json:"confidence"`
	Reasoning            string              `json:"reasoning"`
	CitedClauses         []CitedClause       `json:"cited_clauses"`
	RecommendedActions   []RecommendedAction `json:"recommended_actions"`
	RulesetVersion       string              `json:"ruleset_version"`
	MatchedRules         []string            `json:"matched_rules"`
	LLMUsed              bool                `json:"llm_used"`
}
