package run

func (c *Command) decodePBF() (interface{}, error) {
	return c.PBFDecoder.Decode()
}
