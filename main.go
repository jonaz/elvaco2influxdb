package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb/client"
	"github.com/ryanuber/columnize"
	"github.com/tealeg/xlsx"
)

var elvacoBaseUrl = "http://%s/Elvaco-Rest/rest/"

type Config struct {
	ElvacoServer string
	House        string
	StartDate    string
	User         string
	Password     string
	EndDate      string
}

var config = &Config{}

func main() {
	flag.StringVar(&config.House, "house", "all", "House. Can be commaseparated list ex 100,200,300")
	flag.StringVar(&config.ElvacoServer, "elvacoip", "", "ip address or hostname to elvaco device")
	flag.StringVar(&config.StartDate, "date", "2015-02-01", "start date")
	flag.StringVar(&config.EndDate, "enddate", "", "end date. If set will calculate sum between those dates and print result.")
	flag.StringVar(&config.User, "user", "", "Username")
	flag.StringVar(&config.Password, "password", "", "Password")
	flag.Parse()
	flag.Parse()

	if config.ElvacoServer == "" {
		log.Println("elvacoip is required")
		return
	}
	if config.User == "" {
		log.Println("user is required")
		return
	}
	if config.Password == "" {
		log.Println("password is required")
		return
	}

	if config.EndDate != "" {

		start, err := time.Parse("2006-01-02", config.StartDate)
		if err != nil {
			log.Println(err)
			return
		}
		end, err := time.Parse("2006-01-02", config.EndDate)
		if err != nil {
			log.Println(err)
			return
		}
		printUsageBetweenDates(start, end)
		return
	}

	db := &InfluxDb{}
	db.Connect()

	s := getSeries()

	log.Println(s)

	for _, v := range s {
		//if !strings.HasPrefix(v.SourcePosition, "103,") && !strings.HasPrefix(v.SourcePosition, "303,") {
		if !isAllowedHouse(config.SplitByHouse(), v.SourcePosition) {
			continue
		}

		if v.UnitString == "" {
			continue
		}

		//Workaround not to get err-value from elvaco. they are logged with 0.0.0.3 on volume-flow and power (value 999999)
		if !strings.HasSuffix(v.ApiIdentifier, "0.0.0.0") {
			continue
		}

		log.Printf("%#v\n", v)

		//t := time.Date(2015, time.February, 1, 0, 0, 0, 0, time.UTC)
		t, err := time.Parse("2006-01-02", config.StartDate)
		if err != nil {
			log.Println(err)

			dur,err := time.ParseDuration(config.StartDate)
			if err != nil {
				log.Println(err)
				return
			}

			t = time.Now().UTC().Add(dur)
			t = t.Truncate(24*time.Hour)
			log.Println("TIME", t)

		}

		var next time.Time
		for {
			points := make([]client.Point, 0)
			next = t.Add(time.Hour * 24)
			values := getValues(v.MeasurementSerieId, t, next)
			if next.After(time.Now()) {
				break
			}

			name := v.DeviceTypeString + "_" + v.UnitTypeString + "_" + v.UnitString

			//Aggregate daily for Wh and m3 metrics
			if len(values) == 25 && (strings.HasSuffix(v.UnitString, "Wh") || strings.HasSuffix(v.UnitString, "m3")) {
				diff := values[len(values)-1].Value - values[0].Value
				p := client.Point{
					Measurement: name + "_daily",
					Tags: map[string]string{
						"house": v.SourcePosition,
					},
					Fields: map[string]interface{}{
						"value": diff,
					},
					Time: t,
				}
				points = append(points, p)
			}

			for _, value := range values {
				log.Println(name, MsToTime(value.EffectiveDate), value.Value)

				//Status must be 0 (Valid Data) in order to process it
				if value.Status != 0 || value.Numeric != true {
					continue
				}

				p := client.Point{
					Measurement: name,
					Tags: map[string]string{
						"house": v.SourcePosition,
					},
					Fields: map[string]interface{}{
						"value": value.Value,
					},
					Time: MsToTime(value.EffectiveDate),
				}
				points = append(points, p)
			}
			t = next
			db.Log(points)
		}
	}

}

type Viewmdmserie struct {
	UnitTypeString     string
	SourceType         string
	SourceData         string
	DeviceTypeId       int
	SourceIdentifier   string
	SourceId           int
	DeviceTypeString   string
	UnitString         string
	UnitTypeId         int
	Description        string
	SourcePosition     string
	Name               string
	MeasurementSerieId int
	UnitId             int
	ApiIdentifier      string
	SourceString       string
}

type Mdmdata struct {
	Value         float64
	EffectiveDate int64
	Status        int
	Numeric       bool
}

func getValues(measurementSerieId int, from time.Time, to time.Time) []*Mdmdata {

	url := fmt.Sprintf(elvacoBaseUrl, config.ElvacoServer)
	c := &http.Client{}
	req, err := http.NewRequest("GET", url+"mdmdata/measurementSerieId/"+strconv.Itoa(measurementSerieId)+"/effectiveDate/from/"+strconv.Itoa(int(TimeToMs(from)))+"/to/"+strconv.Itoa(int(TimeToMs(to)))+"/limit/100/offset/0", nil)
	//log.Println(req.URL.String())
	req.SetBasicAuth(config.User, config.Password)
	resp, err := c.Do(req)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	d := json.NewDecoder(resp.Body)

	type Data struct {
		Values []*Mdmdata
	}

	var result Data
	err = d.Decode(&result)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	//for k, v := range result.Values {
	//log.Println(k)
	//log.Printf("%#v\n", v)
	//}

	return result.Values
}
func getSeries() []*Viewmdmserie {

	url := fmt.Sprintf(elvacoBaseUrl, config.ElvacoServer)
	c := &http.Client{}
	req, err := http.NewRequest("GET", url+"viewmdmserie/all", nil)
	req.SetBasicAuth(config.User, config.Password)
	resp, err := c.Do(req)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	d := json.NewDecoder(resp.Body)
	var result []*Viewmdmserie
	err = d.Decode(&result)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	return result
}

type InfluxDb struct {
	conn *client.Client
}

func (i *InfluxDb) Connect() {

	u, err := url.Parse(fmt.Sprintf("http://%s:8086", "localhost"))
	if err != nil {
		log.Println(err)
	}

	conf := client.Config{
		URL:      *u,
		Username: os.Getenv("INFLUX_USER"),
		Password: os.Getenv("INFLUX_PWD"),
	}

	i.conn, err = client.NewClient(conf)
	if err != nil {
		log.Println(err)
		return
	}

	dur, ver, err := i.conn.Ping()
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("Connected to influxdb: %v, %s", dur, ver)

}

func (self *InfluxDb) Log(points []client.Point) {
	//var pts = make([]client.Point, 1)
	//log.Println("sending:", points)
	bps := client.BatchPoints{
		Points:   points,
		Database: "solallen",
	}
	_, err := self.conn.Write(bps)
	if err != nil {
		log.Println(err)
	}
}

func (c *Config) SplitByHouse() []string {
	return strings.Split(c.House, ",")
}

func isAllowedHouse(allowed []string, requested string) bool {
	for _, v := range allowed {
		if v == "all" && isValidHouse(requested) {
			return true
		}
		if v == requested {
			return true
		}
		if strings.HasPrefix(requested, v) {
			return true
		}
	}

	return false
}

func isValidHouse(house string) bool {
	match, err := regexp.MatchString(`^\d\d\d`, house)
	if err != nil {
		log.Println(err)
		return false
	}

	return match
}

func MsToTime(ms int64) time.Time {
	return time.Unix(0, ms*int64(time.Millisecond))
}

func TimeToMs(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

type House struct {
	Power_kWh    float64
	Heat_kWh     float64
	ColdWater_m3 float64
	HotWater_m3  float64
}

func printUsageBetweenDates(start time.Time, end time.Time) {
	allHouses := []string{}
	for i := 1; i <= 7; i++ {
		for x := 1; x <= 3; x++ {
			allHouses = append(allHouses, strconv.Itoa(i)+"0"+strconv.Itoa(x))
		}
	}

	houses := map[string]*House{}

	s := getSeries()
	for _, v := range s {
		//if !strings.HasPrefix(v.SourcePosition, "103,") && !strings.HasPrefix(v.SourcePosition, "303,") {
		if !isAllowedHouse(allHouses, v.SourcePosition) {
			continue
		}

		if v.UnitString == "" {
			continue
		}

		//Workaround not to get err-value from elvaco. they are logged with 0.0.0.3 on volume-flow and power (value 999999)
		if !strings.HasSuffix(v.ApiIdentifier, "0.0.0.0") {
			continue
		}

		name := v.DeviceTypeString + "_" + v.UnitTypeString + "_" + v.UnitString

		if _, ok := houses[v.SourcePosition]; !ok {
			houses[v.SourcePosition] = &House{}
		}

		var err error
		if name == "electricity_energy_kWh" {
			//log.Printf("%#v\n", v)
			//log.Println(v.SourcePosition, name)
			houses[v.SourcePosition].Power_kWh, err = getDiffBetweenTimes(v.MeasurementSerieId, start, end)
			if err != nil {
				log.Println("Error fetching ", v.SourcePosition, name, v.MeasurementSerieId)
				return
			}
		}
		if name == "heat_energy_Wh" {
			val, err := getDiffBetweenTimes(v.MeasurementSerieId, start, end)
			houses[v.SourcePosition].Heat_kWh = val / 1000
			if err != nil {
				log.Println("Error fetching ", v.SourcePosition, name, v.MeasurementSerieId)
				return
			}

		}

		if name == "water_volume_m3" {
			houses[v.SourcePosition].ColdWater_m3, err = getDiffBetweenTimes(v.MeasurementSerieId, start, end)
			if err != nil {
				log.Println("Error fetching ", v.SourcePosition, name, v.MeasurementSerieId)
				return
			}
		}
		if name == "warm water (30°C-90°C)_volume_m3" {
			houses[v.SourcePosition].HotWater_m3, err = getDiffBetweenTimes(v.MeasurementSerieId, start, end)
			if err != nil {
				log.Println("Error fetching ", v.SourcePosition, name, v.MeasurementSerieId)
				return
			}
		}
	}

	output := []string{}
	output = append(output, "house|power|heat|coldwater|hotwater")

	for k, v := range houses {
		tmp := ""
		tmp += k + "|"
		tmp += strconv.FormatFloat(v.Power_kWh, 'f', 2, 64) + "|"
		tmp += strconv.FormatFloat(v.Heat_kWh, 'f', 2, 64) + "|"
		tmp += strconv.FormatFloat(v.ColdWater_m3, 'f', 2, 64) + "|"
		tmp += strconv.FormatFloat(v.HotWater_m3, 'f', 2, 64)
		//fmt.Printf(k)
		//fmt.Printf("\t\t")
		//fmt.Printf(strconv.FormatFloat(v.Power_kWh, 'f', 2, 64))
		//fmt.Printf("\t")
		//fmt.Printf(strconv.FormatFloat(v.Heat_kWh, 'f', 2, 64))
		//fmt.Printf("\t\t")
		//fmt.Printf(strconv.FormatFloat(v.ColdWater_m3, 'f', 2, 64))
		//fmt.Printf("\t")
		//fmt.Printf(strconv.FormatFloat(v.HotWater_m3, 'f', 2, 64))
		//fmt.Printf("\n")
		output = append(output, tmp)
	}

	result := columnize.SimpleFormat(output)
	fmt.Println(result)
	generateExcel(houses)
}

func generateExcel(houses map[string]*House) {

	file := xlsx.NewFile()
	sheet, err := file.AddSheet("Sheet1")
	if err != nil {
		fmt.Println(err)
		return
	}

	row := sheet.AddRow()
	cell := row.AddCell()
	cell.SetString("house")
	cell = row.AddCell()
	cell.SetString("electricity kWh")
	cell = row.AddCell()
	cell.SetString("heat kWh")
	cell = row.AddCell()
	cell.SetString("water m3")
	cell = row.AddCell()
	cell.SetString("hot water m3")

	for k, v := range houses {
		row := sheet.AddRow()
		cell := row.AddCell()
		cell.SetString(k)
		cell = row.AddCell()
		cell.SetFloat(v.Power_kWh)
		cell = row.AddCell()
		cell.SetFloat(v.Heat_kWh)
		cell = row.AddCell()
		cell.SetFloat(v.ColdWater_m3)
		cell = row.AddCell()
		cell.SetFloat(v.HotWater_m3)

	}

	err = file.Save("MyXLSXFile.xlsx")
	if err != nil {
		log.Println(err)
	}
}

func getDiffBetweenTimes(id int, start time.Time, end time.Time) (float64, error) {
	valueStart := getValues(id, start, start.Add(time.Hour))
	valueEnd := getValues(id, end, end.Add(time.Hour))
	if len(valueEnd) == 0 {
		log.Println("error running getValue", valueEnd)
		return 0, errors.New("Error fetching from rest API")
	}
	if len(valueStart) == 0 {
		log.Println("error running getValue", valueStart)
		return 0, errors.New("Error fetching from rest API")
	}
	return valueEnd[0].Value - valueStart[0].Value, nil
}
