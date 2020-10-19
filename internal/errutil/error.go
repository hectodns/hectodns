package errutil

import (
	"github.com/pkg/errors"
)

// Join returns errors grouped into a single error.
func Join(errs ...error) (err error) {
	for _, e := range errs {
		if err == nil {
			err = e
			continue
		}
		if e == nil {
			continue
		}
		err = errors.WithMessage(err, e.Error())
	}
	return
}
