package main

import (
	"github.com/gtopper/v3iocli/pkg/command"
)

func main() {
	rootCmd := command.NewRootCommandeer()
	_ = rootCmd.Execute()
}
