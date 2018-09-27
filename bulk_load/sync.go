package bulk_load

import "errors"

type NotifyHandler func(arg int) (int, error)

var handler NotifyHandler

func RegisterHandler(notifHandler NotifyHandler) {
	handler = notifHandler
}

type NotifyReceiver struct {
}

func (t *NotifyReceiver) Notify(args *int, reply *int) error {
	var e error
	if handler != nil {
		var r int
		r, e = handler(*args)
		*reply = r
	} else {
		e = errors.New("no handler registered")
	}
	return e
}
