package utils

import "fmt"

var Global_DebugPrint bool = false
var Global_LogPrint bool = false

func Enable_DebugPrint() {
	Global_DebugPrint = true
}

func Enable_LogPrint() {
	Global_LogPrint = true
}

func DebugPrint(format string, a ...interface{}) (int, error) {
	if !Global_DebugPrint {
		return 0, nil
	}
	return fmt.Printf(format, a...)
}

func LogPrint(format string, a ...interface{}) (int, error) {
	if !Global_LogPrint {
		return 0, nil
	}
	return fmt.Printf(format, a...)
}
