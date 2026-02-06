package config

import (
	"github.com/gogf/gf/os/glog"
	"testing"
)

func TestLoadYaml(t *testing.T) {
	yaml, err := LoadYaml()
	if err != nil {
		t.Error(err.Error())
	}

	glog.Infof("%+v", yaml)
}
