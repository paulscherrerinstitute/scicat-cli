// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

// very important: there must be an empty line after the build flag line .
package datasetIngestor

import (
	"os"
	"os/user"
	"strconv"
	"syscall"
)

func GetFileOwner(f os.FileInfo) (uidName string, gidName string) {
	uid := strconv.Itoa(int(f.Sys().(*syscall.Stat_t).Uid))
	u, err2 := user.LookupId(uid)
	if err2 != nil {
		uidName = "e" + uid
		// log.Printf("Warning: unknown user name for file %s, assume e-account %s", f.Name(), uidName)
	} else {
		uidName = u.Username
	}

	gid := strconv.Itoa(int(f.Sys().(*syscall.Stat_t).Gid))
	g, err := user.LookupGroupId(gid)
	if err != nil {
		gidName = gid
		// log.Printf("Warning: unknown group name for file %s, set to gid number %s", f.Name(), gidName)
	} else {
		gidName = g.Name
	}
	return uidName, gidName
}
