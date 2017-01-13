package run

import (
	"encoding/json"
	"io"
	"log"

	"github.com/ambiweb/osm-pbf-filter/tags"
	"github.com/qedus/osmpbf"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var collectedKeyPrefix = []byte("collected")

// Command represents an environment and settings for a command to run.
type Command struct {
	PBFDecoder  *osmpbf.Decoder
	LevelDB     *leveldb.DB
	TagsMatcher tags.Matcher
	Stdout      io.Writer
}

// Run executes main logic.
func Run(c *Command) error {
	log.Print("Start transfering data from PBF to levelDB")
	if err := c.PutData(); err != nil {
		return err
	}
	log.Print("Start collecting related items")
	if err := c.CollectRelated(); err != nil {
		return err
	}
	log.Print("Preparing to output JSON")
	if err := c.OutputJSON(); err != nil {
		return err
	}
	return nil
}

// PutData reads data from PBF and saves it in levelDB.
// If data item matches tags, it is saved as collected.
func (c *Command) PutData() error {
	return c.TraverseData(func(v interface{}) error {
		fn := c.Put
		if c.TagsMatch(v) {
			fn = c.Collect
		}
		if err := fn(v); err != nil {
			return err
		}
		return nil
	})
}

// TraverseDataFunc is a function to use with TraverseData Command method.
type TraverseDataFunc func(interface{}) error

// TraverseData loops through the data and executes function on every data item.
func (c *Command) TraverseData(fn TraverseDataFunc) error {
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

// CollectRelated marks related values of previously collected items as collected.
func (c *Command) CollectRelated() error {
	return c.TraverseCollected(func(k []byte, v interface{}) error {
		switch v := v.(type) {
		case *osmpbf.Relation:
			log.Print("Relation found. Collecting members")
			if err := c.collectMembers(v.Members); err != nil {
				return err
			}
		}
		return nil
	})
}

// TraverseCollectedFunc is a function to use with TraverseCollected Command method.
type TraverseCollectedFunc func(k []byte, v interface{}) error

// TraverseCollected loops through the collected items and executes function on every item.
func (c *Command) TraverseCollected(fn TraverseCollectedFunc) error {
	iter := c.LevelDB.NewIterator(util.BytesPrefix(collectedKeyPrefix), nil)
	defer iter.Release()
	for iter.Next() {
		log.Print("Got collected item. Decoding...")
		var value interface{}
		if err := json.Unmarshal(iter.Value(), &value); err != nil {
			return err
		}
		if err := fn(iter.Key(), value); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (c *Command) collectMembers(members []osmpbf.Member) error {
	for _, m := range members {
		dbKey := &DBKey{Type: m.Type, ID: m.ID}
		key, err := dbKey.Bytes()
		if err != nil {
			return err
		}
		value, err := c.dbGet(key)
		if err != nil {
			return err
		}
		if err := c.dbDelete(key); err != nil {
			return err
		}
		key = append(collectedKeyPrefix, key...)
		if err := c.dbPut(key, value); err != nil {
			return err
		}

		if m.Type == osmpbf.RelationType {
			var v osmpbf.Relation
			if err := json.Unmarshal(value, &v); err != nil {
				return err
			}
			if err := c.collectMembers(v.Members); err != nil {
				return err
			}
		}
	}
	return nil
}

// OutputJSON outputs collected entries as JSON.
func (c *Command) OutputJSON() error {
	io.WriteString(c.Stdout, "[")
	comma := ""
	err := c.TraverseCollectedRaw(func(_, v []byte) error {
		io.WriteString(c.Stdout, comma)
		c.Stdout.Write(v)
		if comma == "" {
			comma = ","
		}
		return nil
	})
	if err != nil {
		return err
	}
	io.WriteString(c.Stdout, "]")
	return nil
}

// TraverseCollectedRawFunc is a function to use with TraverseCollectedRaw Command method.
type TraverseCollectedRawFunc func(k, v []byte) error

// TraverseCollectedRaw loops through the collected items and executes function on every item.
func (c *Command) TraverseCollectedRaw(fn TraverseCollectedRawFunc) error {
	iter := c.LevelDB.NewIterator(util.BytesPrefix(collectedKeyPrefix), nil)
	defer iter.Release()
	for iter.Next() {
		if err := fn(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}
