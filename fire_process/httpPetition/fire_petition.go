package fire

import (
	"encoding/csv"
	"fmt"
	"main/http/models"
	"net/http"
	"strconv"
)

var SATELITE_LIST = []string{
	"VIIRS_SNPP_NRT",
	"VIIRS_NOAA20_NRT",
	"VIIRS_NOAA21_NRT",
	"MODIS_NRT",
	"MODIS_SP",
}

var MAP_KEY = "1d776a369ffd5e3c4d5842ec77c7146d"

func FirePetition(count int) ([]models.FireData, error) {
	// Nos encargamos de hacer la peticion a la api de los mapas
	if count == 0 {
		return nil, fmt.Errorf("No se puede ejecutar en este momento")
	}

	if count > len(SATELITE_LIST) {
		count = len(SATELITE_LIST)
	}

	var fires []models.FireData

	for i := 0; i < count; i++ {
		satelite := SATELITE_LIST[i]
		url := fmt.Sprintf(
			"https://firms.modaps.eosdis.nasa.gov/api/area/csv/%s/%s/-10,35,5,44/1/2026-11-25", //2026-07-20, --> En esta fecha estamos convencidos de que hubo incendios
			MAP_KEY,
			satelite,
		)

		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("error HTTP: %d", resp.StatusCode)
		}

		reader := csv.NewReader(resp.Body)
		records, err := reader.ReadAll()
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		// Saltamos la cabecera
		for _, row := range records[1:] {
			latitude, err := strconv.ParseFloat(row[0], 64)
			if err != nil {
				return nil, err
			}

			longitude, err := strconv.ParseFloat(row[1], 64)
			if err != nil {
				return nil, err
			}

			brightness, err := strconv.ParseFloat(row[2], 64)
			if err != nil {
				return nil, err
			}

			brightT31, err := strconv.ParseFloat(row[11], 64)
			if err != nil {
				return nil, err
			}

			frp, err := strconv.ParseFloat(row[12], 64)
			if err != nil {
				return nil, err
			}

			fire := models.FireData{
				Latitude:   latitude,
				Longitude:  longitude,
				Brightness: brightness,
				AcqDate:    row[5],
				AcqTime:    row[6],
				Confidence: row[9],
				BrightT31:  brightT31,
				FRP:        frp,
				DayNight:   row[13],
			}

			fires = append(fires, fire)
		}
	}
	return fires, nil
}
