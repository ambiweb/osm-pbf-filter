package tags

import "sort"

// Matcher represents a tags structure to match.
type Matcher map[string]interface{}

// Match checks if tags match.
func (m Matcher) Match(tags map[string]string) bool {
	for k, v := range m {
		switch v := v.(type) {
		case bool:
			if _, ok := tags[k]; ok && v {
				return true
			}
		case []string:
			if tag, ok := tags[k]; ok {
				sort.Strings(v)
				i := sort.Search(len(v), func(i int) bool { return v[i] >= tag })
				if i < len(v) && v[i] == tag {
					return true
				}
			}
		case string:
			if tag, ok := tags[k]; ok && v == tag {
				return true
			}
		}
	}
	return false
}
