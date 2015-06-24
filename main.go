package main

import (
	"encoding/json"
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
)

var elvacoBaseUrl = "http://%s/Elvaco-Rest/rest/"

type Config struct {
	ElvacoServer string
	House        string
	StartDate    string
	User         string
	Password     string
}

var config = &Config{}

func main() {
	flag.StringVar(&config.House, "house", "all", "House. Can be commaseparated list ex 100,200,300")
	flag.StringVar(&config.ElvacoServer, "elvacoip", "", "ip address or hostname to elvaco device")
	flag.StringVar(&config.StartDate, "date", "2015-02-01", "start date")
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
			return
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
			if strings.HasSuffix(v.UnitString, "Wh") || strings.HasSuffix(v.UnitString, "m3") {
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
