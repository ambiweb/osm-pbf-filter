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
