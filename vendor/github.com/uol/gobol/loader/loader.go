package loader

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
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

func ConfToml(path string, settings interface{}) error {

	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	_, err = toml.DecodeFile(absolutePath, settings)

	return err
}
