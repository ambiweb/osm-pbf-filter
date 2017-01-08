package run

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/qedus/osmpbf"
)

// DBKey represents a key for a levelDB record.
type DBKey struct {
	Type osmpbf.MemberType
	ID   int64
}

// Bytes returns bytes representation of DBKey.
func (k *DBKey) Bytes() []byte {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)

	if err := enc.Encode(k); err != nil {
		return nil
	}

	return buffer.Bytes()
}

// KeyValue returns key and value for a levelDB record.
func KeyValue(v interface{}) (key, value []byte, err error) {
	dbKey := &DBKey{}
	switch v := v.(type) {
	case *osmpbf.Node:
		dbKey.Type = osmpbf.NodeType
		dbKey.ID = v.ID
	case *osmpbf.Way:
		dbKey.Type = osmpbf.WayType
		dbKey.ID = v.ID
	case *osmpbf.Relation:
		dbKey.Type = osmpbf.RelationType
		dbKey.ID = v.ID
	default:
		return nil, nil, fmt.Errorf("unknown type %T\n", v)
	}

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)

	if err := enc.Encode(dbKey); err != nil {
		return nil, nil, err
	}
	key = buffer.Bytes()

	buffer.Reset()

	if err := enc.Encode(v); err != nil {
		return nil, nil, err
	}
	value = buffer.Bytes()

	return key, value, nil
}

func (c *Command) dbPut(key, value []byte) error {
	return c.LevelDB.Put(key, value, nil)
}

func (c *Command) dbGet(key []byte) (value []byte, err error) {
	return c.LevelDB.Get(key, nil)
}

func (c *Command) dbDelete(key []byte) error {
	return c.LevelDB.Delete(key, nil)
}

func detectType(key []byte) (osmpbf.MemberType, error) {
	buffer := bytes.NewBuffer(bytes.TrimPrefix(key, collectedKeyPrefix))
	dec := gob.NewDecoder(buffer)
	var dbKey DBKey
	if err := dec.Decode(&dbKey); err != nil {
		return -1, err
	}
	return dbKey.Type, nil
}
