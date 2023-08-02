package config

import (
	"bufio"
	"log"
	"my-godis/src/lib/logger"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type PropertyHolder struct {
	Bind           string `cfg:"bind"`
	Port           int    `cfg:"port"`
	AppendOnly     bool   `cfg:"appendOnly"`
	AppendFilename string `cfg:"appendFilename"`
	MaxClients     int    `cfg:"maxclients"`
}

var Properties *PropertyHolder

func init() {
	// default config
	Properties = &PropertyHolder{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
	}
}

func LoadConfig(configFilename string) *PropertyHolder {
	// open config file
	config := &PropertyHolder{
		Bind:           "127.0.0.1",
		Port:           6379,
		AppendOnly:     true,
		AppendFilename: "appendonly.aof",
	}
	file, err := os.Open(configFilename)
	if err != nil {
		log.Print(err)
		return config
	}
	defer file.Close()

	// read config file
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 { // separator found
			key := line[0:pivot]
			value := line[pivot+1:]
			rawMap[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				boolValue := "yes" == value
				fieldVal.SetBool(boolValue)
			}
		}
	}
	return config
}

func SetupConfig(configFilename string) {
	Properties = LoadConfig(configFilename)
}
