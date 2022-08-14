package server

import (
	"github.com/blacktear23/dragonbolt/protocol"
	"github.com/blacktear23/dragonbolt/tso"
)

func (c *rclient) handleCreateDB(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	name := string(key)
	have, _, err := c.hasDB(name)
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	if have {
		return protocol.NewSimpleError("Already exists")
	}

	info, err := c.rs.tsoSrv.CreateDB(name)
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	err = c.rs.tsoSrv.UpDB(info)
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	return protocol.NewSimpleString("OK")
}

func (c *rclient) hasDB(name string) (bool, *tso.DBInfo, error) {
	if name == "default" {
		return true, nil, nil
	}
	infos, err := c.rs.tsoSrv.ListDB()
	if err != nil {
		return false, nil, err
	}
	for _, info := range infos {
		if info.Name == name {
			return true, info, nil
		}
	}
	return false, nil, nil
}

func (c *rclient) handleUseDB(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	name := string(key)
	if name == "default" {
		// Restore to default
		c.sid = c.rs.shardID
		return protocol.NewSimpleString("OK")
	}

	// List db
	infos, err := c.rs.tsoSrv.ListDB()
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	for _, info := range infos {
		if info.Name == name {
			c.sid = info.ShardID
			return protocol.NewSimpleString("OK")
		}
	}
	return protocol.NewSimpleError("DB not found")
}

func (c *rclient) handleListDB(args []protocol.Encodable) protocol.Encodable {
	infos, err := c.rs.tsoSrv.ListDB()
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	ret := protocol.Array{
		protocol.NewBlobStringFromString("default"),
	}
	for _, info := range infos {
		ret = append(ret, protocol.NewBlobStringFromString(info.Name))
	}
	return ret
}

func (c *rclient) handleCurrentDB(args []protocol.Encodable) protocol.Encodable {
	if c.sid == c.rs.shardID {
		return protocol.NewBlobStringFromString("default")
	}
	infos, err := c.rs.tsoSrv.ListDB()
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	for _, info := range infos {
		if info.ShardID == c.sid {
			return protocol.NewBlobStringFromString(info.Name)
		}
	}
	return protocol.NewSimpleErrorf("Name not known for shard %d", c.sid)
}

func (c *rclient) handleDeleteDB(args []protocol.Encodable) protocol.Encodable {
	if len(args) < 1 {
		return protocol.NewSimpleError("Need more arguments")
	}
	key, err := c.parseKey(args[0])
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	name := string(key)
	if name == "default" {
		return protocol.NewSimpleError("Cannot delete default db")
	}
	have, info, err := c.hasDB(name)
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	if !have {
		return protocol.NewSimpleError("DB not exists")
	}
	err = c.rs.tsoSrv.DownDB(info)
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}

	err = c.rs.tsoSrv.DeleteDB(info.Name)
	if err != nil {
		return protocol.NewSimpleError(err.Error())
	}
	return protocol.NewSimpleString("OK")
}
