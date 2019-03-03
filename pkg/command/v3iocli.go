package command

import (
	"encoding/base64"
	"errors"
	"github.com/spf13/cobra"
	"net/url"
	"os"
	"strings"
)

type printUsageError error

type RootCommandeer struct {
	cmd           *cobra.Command
	server        string
	user          string
	password      string
	token         string
	authorization string
}

func NewRootCommandeer() *RootCommandeer {
	cmd := &cobra.Command{
		Use:   "v3iocli [command] [arguments] [flags]",
		Short: "V3IO command-line interface",
	}

	commandeer := &RootCommandeer{
		cmd: cmd,
	}

	defaultV3ioServer := os.Getenv("V3IO_API")

	cmd.PersistentFlags().StringVarP(&commandeer.server, "server", "s", defaultV3ioServer, "V3IO API address")
	cmd.PersistentFlags().StringVarP(&commandeer.user, "user", "u", "", "user name")
	cmd.PersistentFlags().StringVarP(&commandeer.password, "password", "p", "", "password")
	cmd.PersistentFlags().StringVarP(&commandeer.token, "token", "t", "", "V3IO access key (session token)")

	cmd.AddCommand(
		newGetItemsCommandeer(commandeer).cmd,
	)

	return commandeer
}

func (r *RootCommandeer) Execute() error {
	return r.cmd.Execute()
}

// Parse server as a URL if it starts with an http or https protocol. Otherwise, assume no protocol is provided and use http.
func (r *RootCommandeer) buildUrl() (*url.URL, error) {
	sanitizedPath := r.server
	if !strings.HasPrefix(sanitizedPath, "http://") && !strings.HasPrefix(sanitizedPath, "https://") {
		sanitizedPath = "http://" + sanitizedPath
	}
	urlPath, err := url.Parse(sanitizedPath)
	if err != nil {
		return nil, err
	}
	return urlPath, nil
}

func (r *RootCommandeer) init() {
	if r.token == "" && r.password == "" { // Only use env if no credentials are provided as CLI options.
		r.token = os.Getenv("V3IO_ACCESS_KEY")
		r.user = os.Getenv("V3IO_USERNAME")
		r.password = os.Getenv("V3IO_PASSWORD")
	}
	r.authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte(r.user+":"+r.password))
	r.password = ""
}

func (r *RootCommandeer) verify() error {
	r.init()
	if r.server == "" {
		return errors.New("please specify --server or define V3IO_API")
	}
	return nil
}
