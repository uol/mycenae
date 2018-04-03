package loader

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/hashicorp/consul/api"
	yaml "gopkg.in/yaml.v2"
)

func ConfJson(path string, settings interface{}) error {

	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	confFile, err := os.Open(absolutePath)
	if err != nil {
		return err
	}

	return json.NewDecoder(confFile).Decode(&settings)
}

func ConfYaml(path string, settings interface{}) error {

	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	confFile, err := ioutil.ReadFile(absolutePath)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(confFile, settings)
}

func ConfConsul(path string, token string, settings interface{}) error {

	config := api.DefaultConfig()

	client, err := api.NewClient(config)
	if err != nil {
		return err
	}

	pair, _, err := client.KV().Get(path, nil)
	if err != nil {
		return err
	}

	return json.Unmarshal(pair.Value, settings)
}

func ConfToml(path string, settings interface{}) error {

	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	_, err = toml.DecodeFile(absolutePath, settings)

	return err
}
