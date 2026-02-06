package dbmgr

import (
	"os"
	"reflect"
	"testing"
)

func Test_sendEvents(t *testing.T) {
	os.Setenv("AZCOPY_AUTO_LOGIN_TYPE", "MSI")
	tests := []struct {
		name string
		want *eventExample
	}{
		// TODO: Add test cases.
		{"test", &eventExample{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sendEvents(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sendEvents() = %v, want %v", got, tt.want)
			}
		})
	}
}
