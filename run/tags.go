package run

import (
	"github.com/qedus/osmpbf"
)

func (c *Command) tagsMatch(v interface{}) bool {
	switch v := v.(type) {
	case *osmpbf.Node:
	case *osmpbf.Way:
	case *osmpbf.Relation:
		return c.TagsMatcher.Match(v.Tags)
	}
	return false
}
