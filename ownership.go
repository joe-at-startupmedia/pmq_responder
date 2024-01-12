package pmq_responder

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
)

type Ownership struct {
	Group    string
	Username string
}

type User struct {
	Uid int
	Gid int
}

type Group struct {
	Gid int
}

func idToInt(idStr string) int {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		// ... handle error
		panic(err)
	}
	return id
}

func (o *Ownership) HasUser() (bool, *User, error) {
	if len(o.Username) > 0 {
		osUser, err := user.Lookup(o.Username)

		if err != nil {
			return false, nil, err
		}
		mqUser := User{
			Uid: idToInt(osUser.Uid),
			Gid: idToInt(osUser.Gid),
		}
		return true, &mqUser, nil
	}
	return false, nil, nil
}

func (o *Ownership) HasGroup() (bool, *Group, error) {
	if len(o.Group) > 0 {
		osGroup, err := user.LookupGroup(o.Group)
		if err != nil {
			return false, nil, err
		}
		mqGroup := Group{
			Gid: idToInt(osGroup.Gid),
		}
		return true, &mqGroup, nil
	}
	return false, nil, nil
}

func (o *Ownership) IsValid() bool {

	hasGroup, _, err := o.HasGroup()
	if err != nil {
		return false
	}
	hasUser, _, err := o.HasUser()
	if err != nil {
		return false
	}
	if hasGroup && !hasUser {
		fmt.Println("Cannot infer user from the group alone")
		return false
	}
	return hasGroup && hasUser
}

func (o *Ownership) ApplyPermissions(config *QueueConfig) error {

	hasGroup, group, err := o.HasGroup()
	if err != nil {
		return errors.New("Cannot get group")
	}
	hasUser, user, err := o.HasUser()
	if err != nil {
		return errors.New("Cannot get user")
	}

	if hasGroup || hasUser {
		err = os.Chmod(config.GetFile(), 0660)
	} else {
		return os.Chmod(config.GetFile(), 0666)
	}
	if hasGroup && hasUser {
		err = os.Chown(config.GetFile(), user.Gid, group.Gid)
	} else if hasUser {
		err = os.Chown(config.GetFile(), user.Gid, user.Gid)
	}

	return err
}
