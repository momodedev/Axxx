package dbmgr

import (
	"reflect"
	"testing"
)

func Test_tests3available(t *testing.T) {
	tests := []struct {
		name string
		want *s3test
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tests3available(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("tests3available() = %v, want %v", got, tt.want)
			}
		})
	}
}
