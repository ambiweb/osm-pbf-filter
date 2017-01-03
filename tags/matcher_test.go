package tags_test

import (
	"testing"

	"github.com/ambiweb/osm-pbf-filter/tags"
)

var matchTests = []struct {
	matcher  tags.Matcher
	tags     map[string]string
	expected bool
}{
	{
		tags.Matcher(map[string]interface{}{"tag": true}),
		map[string]string{"tag": ""},
		true,
	},
	{
		tags.Matcher(map[string]interface{}{"tag": false}),
		map[string]string{"tag": ""},
		false,
	},
	{
		tags.Matcher(map[string]interface{}{"tag": []string{"value"}}),
		map[string]string{"tag": "value"},
		true,
	},
	{
		tags.Matcher(map[string]interface{}{"tag": []string{"value"}}),
		map[string]string{"tag": "another value"},
		false,
	},
	{
		tags.Matcher(map[string]interface{}{"tag": "value"}),
		map[string]string{"tag": "value"},
		true,
	},
	{
		tags.Matcher(map[string]interface{}{"tag": "value"}),
		map[string]string{"tag": "another value"},
		false,
	},
}

func TestMatch(t *testing.T) {
	for _, tt := range matchTests {
		if actual := tt.matcher.Match(tt.tags); actual != tt.expected {
			t.Errorf("Expected %v, actual %v", tt.expected, actual)
		}
	}
}
