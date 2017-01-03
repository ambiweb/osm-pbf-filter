package run

import (
	"io"

	"github.com/ambiweb/osm-pbf-filter/tags"
	"github.com/qedus/osmpbf"
	"github.com/syndtr/goleveldb/leveldb"
)

var collectedKeyPrefix = []byte("collected")

// Command represents an environment and settings for a command to run.
type Command struct {
	PBFDecoder  *osmpbf.Decoder
	LevelDB     *leveldb.DB
	TagsMatcher tags.Matcher
}

// Run executes main logic.
func Run(c *Command) error {
	err := c.TraverseData(func(v interface{}) error {
		fn := c.Put
		if c.TagsMatch(v) {
			fn = c.Collect
		}
		if err := fn(v); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// TraverseFunc is a function to use with TraverseData Command method.
type TraverseFunc func(interface{}) error

// TraverseData loops through the data and executes function on every data item.
func (c *Command) TraverseData(fn TraverseFunc) error {
	for {
		v, err := c.decodePBF()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		if err := fn(v); err != nil {
			return err
		}
	}
}

// Put stores value in a key-value store.
func (c *Command) Put(v interface{}) error {
	key, value, err := KeyValue(v)
	if err != nil {
		return err
	}
	return c.dbPut(key, value)
}

// TagsMatch checks if Tags match the tags matching rules.
func (c *Command) TagsMatch(v interface{}) bool {
	return c.tagsMatch(v)
}

// Collect stores value in a key-value store indicating it as collected.
func (c *Command) Collect(v interface{}) error {
	key, value, err := KeyValue(v)
	if err != nil {
		return err
	}
	key = append(collectedKeyPrefix, key...)
	return c.dbPut(key, value)
}
