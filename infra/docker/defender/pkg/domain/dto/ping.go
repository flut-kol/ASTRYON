package dto

type Ping struct {
	IsHealthy bool `json:"IsHealthy"`
}

func MakeUnhealthyPing() Ping {
	return Ping{IsHealthy: false}
}
