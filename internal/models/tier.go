package models

// Tier represents a pricing tier from tiers.json
type Tier struct {
	Tier      int      `json:"tier"`
	Tags      []string `json:"tags"`
	Currency  string   `json:"currency"`
	PriceReg  *float64 `json:"price_reg,omitempty"`
	PriceRen  *float64 `json:"price_ren,omitempty"`
	PriceRes  *float64 `json:"price_res,omitempty"`
}

