// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

// very important: there must be an empty line after the build flag line .
package datasetIngestor

import (
	"os"
	"os/user"
	"strconv"
	"syscall"
)

/* GetFileOwner retrieves the owner of a given file. It takes an os.FileInfo object as input
and returns two strings: the username of the file's owner and the name of the group that owns the file.
If the user or group cannot be determined, it returns the user ID or group ID as a string.
If there is an error during the lookup of the user or group, it prefixes the user ID with "e" and uses the group ID as is.*/
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
