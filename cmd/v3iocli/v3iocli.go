package main

import (
	"fmt"
	"github.com/gtopper/v3iocli/pkg/command"
	"os"
)

func main() {
	rootCmd := command.NewRootCommandeer()
	err := rootCmd.Execute()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
	}
}
