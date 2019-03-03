package command

import (
	"encoding/base64"
	"github.com/spf13/cobra"
	"net/url"
	"os"
	"strings"
)

type RootCommandeer struct {
	cmd           *cobra.Command
	v3ioPath      string
	user          string
	password      string
	token         string
	authorization string
}

func NewRootCommandeer() *RootCommandeer {
	cmd := &cobra.Command{
		Use:          "v3iocli [command] [arguments] [flags]",
		Short:        "V3IO command-line interface",
		SilenceUsage: true,
	}

	commandeer := &RootCommandeer{
		cmd: cmd,
	}

	defaultV3ioServer := os.Getenv("V3IO_API")

	cmd.PersistentFlags().StringVarP(&commandeer.v3ioPath, "server", "s", defaultV3ioServer, "V3IO API address")
	cmd.PersistentFlags().StringVarP(&commandeer.user, "user", "u", "", "User name")
	cmd.PersistentFlags().StringVarP(&commandeer.password, "password", "p", "", "Password")
	cmd.PersistentFlags().StringVarP(&commandeer.token, "token", "t", "", "V3IO access key (session token)")

	cmd.AddCommand(
		newGetItemsCommandeer(commandeer).cmd,
	)

	return commandeer
}

func (r *RootCommandeer) Execute() error {
	return r.cmd.Execute()
}

// Parse v3ioPath as a URL if it starts with an http or https protocol. Otherwise, assume no protocol is provided and use http.
func (r *RootCommandeer) buildUrl() (*url.URL, error) {
	sanitizedPath := r.v3ioPath
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
