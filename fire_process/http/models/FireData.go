package models

type FireData struct {
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	Brightness float64 `json:"brightness"`
	AcqDate    string  `json:"acq_date"`
	AcqTime    string  `json:"acq_time"`
	Confidence string  `json:"confidence"`
	BrightT31  float64 `json:"bright_t31"`
	FRP        float64 `json:"frp"`
	DayNight   string  `json:"daynight"`
}
