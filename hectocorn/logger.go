package hectocorn

import (
	"fmt"
	"io"
	"os"
)

var Log = Logger{Device: os.Stderr}

type Logger struct {
	Device io.Writer
}

func (l *Logger) log(level, format string, a ...interface{}) {
	l.Device.Write([]byte(level + ":" + fmt.Sprintf(format, a...) + "\n"))
}

func (l *Logger) Debug(format string, a ...interface{}) {
	l.log("d", format, a...)
}

func (l *Logger) Info(format string, a ...interface{}) {
	l.log("i", format, a...)
}

func (l *Logger) Warn(format string, a ...interface{}) {
	l.log("w", format, a...)
}

func (l *Logger) Error(format string, a ...interface{}) {
	l.log("e", format, a...)
}

// Fatal is equivalent to Error() followed by a call to os.Exit(1).
func (l *Logger) Fatal(format string, a ...interface{}) {
	l.log("e", format, a...)
	os.Exit(1)
}
