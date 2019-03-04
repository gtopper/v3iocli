package command

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/valyala/fasthttp"
	"io"
	"net/url"
	"os"
	"path"
	"strconv"
)

const workerQueueSize = 64

type PutItemsCommandeer struct {
	rootCommandeer *RootCommandeer
	cmd            *cobra.Command
	path           string
	targetUrl      string
	format         string
	key            string
	numWorkers     string
}

func newPutItemsCommandeer(rootCommandeer *RootCommandeer) *PutItemsCommandeer {

	commandeer := &PutItemsCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "putitems <path>",
		Short: "Write v3io tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 || args[0] == "" {
				return errors.New("path must be specified")
			}
			commandeer.path = args[0]
			err := commandeer.execute()
			if err != nil {
				if _, ok := err.(printUsageError); ok {
					return err
				}
				_, _ = fmt.Fprintln(os.Stderr, err)
			}
			return nil
		},
	}

	cmd.PersistentFlags().StringVarP(&commandeer.format, "input", "i", "json", "Input format")
	cmd.PersistentFlags().StringVarP(&commandeer.key, "key", "k", "", "Key (CSV input only)")
	cmd.PersistentFlags().StringVarP(&commandeer.numWorkers, "workers", "w", "16", "Number of workers")

	commandeer.cmd = cmd

	return commandeer
}

func (p *PutItemsCommandeer) execute() error {
	err := p.rootCommandeer.verify()
	if err != nil {
		return printUsageError{err}
	}
	if p.format == "csv" && p.key == "" {
		return printUsageError{errors.New("'--input csv' must be specified in conjunction with --key")}
	}

	targetUrl, err := p.buildUrl()
	if err != nil {
		return err
	}
	p.targetUrl = targetUrl.String()
	numWorkers, err := strconv.Atoi(p.numWorkers)
	if err != nil {
		return printUsageError{errors.Wrapf(err, "could not parse number of workers from '%s'", p.numWorkers)}
	}
	var workerChannels = make([]chan []byte, numWorkers)
	var terminationChannels = make([]chan error, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workerChannels[i] = make(chan []byte, workerQueueSize)
		terminationChannels[i] = make(chan error, 1)
		go p.worker(workerChannels[i], terminationChannels[i])
	}

	in := os.Stdin
	var reader itemReader
	switch p.format {
	case "json":
		reader = &jsonReader{under: bufio.NewReader(in)}
	case "csv":
		reader, err = newCsvReader(in, p.key)
		if err != nil {
			return err
		}
	default:
		return errors.Errorf("invalid format '%s'", p.format)
	}
	i := 0
readLoop:
	for {
		for i := 0; i < numWorkers; i++ {
			if len(terminationChannels[i]) > 0 {
				break readLoop
			}
		}
		record, err := reader.read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		workerChannels[i] <- record
		i++
		if i%numWorkers == 0 {
			i = 0
		}
	}
	for i := 0; i < numWorkers; i++ {
		close(workerChannels[i])
	}
	for i := 0; i < numWorkers; i++ {
		terminationResult := <-terminationChannels[i]
		if err == nil {
			err = terminationResult
		}
	}
	return err
}

func (p *PutItemsCommandeer) worker(ch <-chan []byte, termination chan<- error) {
	var err error
	for entry := range ch {
		err = p.sendRequest(entry)
		if err != nil {
			termination <- err
			return
		}
	}
	termination <- nil
}

func (p *PutItemsCommandeer) sendRequest(entry []byte) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(p.targetUrl)
	req.Header.SetMethod("PUT")
	if p.rootCommandeer.token == "" {
		req.Header.Set("Authorization", p.rootCommandeer.authorization)
	} else {
		req.Header.Set("X-v3io-session-key", p.rootCommandeer.token)
	}
	req.Header.Set("X-v3io-function", "PutItem")
	req.SetBody(entry)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	err := fasthttp.Do(req, resp)
	if err != nil {
		return errors.Wrap(err, "failed to send request")
	}
	if resp.StatusCode() != 200 && resp.StatusCode() != 204 {
		body := string(resp.Body())
		if body != "" {
			body = ": \n" + body + "\n"
		}
		return errors.Errorf("got unexpected response code %d from %s%s for request:\n %s", resp.StatusCode(), p.targetUrl, body, req)
	}
	return nil
}

// Same as RootCommandeer.buildUrl() but with a canonical, slash-terminated path appended.
func (g *PutItemsCommandeer) buildUrl() (*url.URL, error) {
	baseUrl, err := g.rootCommandeer.buildUrl()
	if err != nil {
		return nil, err
	}
	baseUrl.Path = path.Clean(baseUrl.Path+"/"+g.path) + "/"
	return baseUrl, nil
}
