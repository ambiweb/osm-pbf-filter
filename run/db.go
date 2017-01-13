package run

import (
	"encoding/json"
	"fmt"

	"github.com/qedus/osmpbf"
)

// DBKey represents a key for a levelDB record.
type DBKey struct {
	Type osmpbf.MemberType `json:"t"`
	ID   int64             `json:"i"`
}

// Bytes returns byte slice representation of a key.
func (key *DBKey) Bytes() ([]byte, error) {
	return json.Marshal(key)
}

// KeyValue returns key and value for a levelDB record.
func KeyValue(v interface{}) (key, value []byte, err error) {
	var dbKey *DBKey
	switch v := v.(type) {
	case *osmpbf.Node:
		dbKey = &DBKey{
			Type: osmpbf.NodeType,
			ID:   v.ID,
		}
	case *osmpbf.Way:
		dbKey = &DBKey{
			Type: osmpbf.WayType,
			ID:   v.ID,
		}
	case *osmpbf.Relation:
		dbKey = &DBKey{
			Type: osmpbf.RelationType,
			ID:   v.ID,
		}
	default:
		return nil, nil, fmt.Errorf("unknown type %T", v)
	}

	if key, err = json.Marshal(dbKey); err != nil {
		return nil, nil, err
	}

	if value, err = json.Marshal(v); err != nil {
		return nil, nil, err
	}

	return
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
