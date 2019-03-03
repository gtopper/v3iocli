// The name of this file is prefixed with an x because GoLand bizzarrely refuses to treat getitems.go as a Go file.

package command

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/valyala/fasthttp"
	"net/url"
	"path"
)

type GetItemsCommandeer struct {
	rootCommandeer *RootCommandeer
	cmd            *cobra.Command
	path           string
	targetUrl      string
	format         string
	attributes     string
	filter         string
}

type getItemsRequest struct {
	Marker           string `json:"Marker,omitempty"`
	AttributesToGet  string `json:"AttributesToGet,omitempty"`
	FilterExpression string `json:"FilterExpression,omitempty"`
}

type getItemsResponse struct {
	LastItemIncluded string
	NextMarker       string
	NumItems         int
	Items            []Item
}

type Item map[string]map[string]string

func newGetItemsCommandeer(rootCommandeer *RootCommandeer) *GetItemsCommandeer {

	commandeer := &GetItemsCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:   "getitems",
		Short: "Read v3io tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 || args[0] == "" {
				return errors.New("path must not be empty")
			}
			commandeer.path = args[0]
			return commandeer.execute()
		},
	}

	cmd.PersistentFlags().StringVarP(&commandeer.format, "output", "o", "json", "Output format")
	cmd.PersistentFlags().StringVarP(&commandeer.attributes, "attributes", "a", "", "Attributes to request")
	cmd.PersistentFlags().StringVarP(&commandeer.filter, "filter", "f", "", "Filter expression")

	commandeer.cmd = cmd

	return commandeer
}

func (g *GetItemsCommandeer) execute() error {
	g.rootCommandeer.init()

	targetUrl, err := g.buildUrl()
	g.targetUrl = targetUrl.String()

	if err != nil {
		return err
	}

	marker := ""
	var out itemWritter
	switch g.format {
	case "csv":
		out = NewCsvItemWritter()
	case "json":
		out = &jsonItemWritter{}
	default:
		return errors.Errorf("unsupported output format: %s", g.format)
	}
	for {
		resp, err := g.makeRequest(marker)
		if err != nil {
			return err
		}
		for _, item := range resp.Items {
			err := out.Write(item)
			if err != nil {
				return err
			}
		}
		if resp.LastItemIncluded == "TRUE" {
			break
		}
		marker = resp.NextMarker
	}

	return nil
}

func (g *GetItemsCommandeer) makeRequest(marker string) (*getItemsResponse, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(g.targetUrl)
	req.Header.SetMethod("PUT")
	if g.rootCommandeer.token == "" {
		req.Header.Set("Authorization", g.rootCommandeer.authorization)
	} else {
		req.Header.Set("X-v3io-session-key", g.rootCommandeer.token)
	}
	req.Header.Set("X-v3io-function", "GetItems")
	getItemsReq := getItemsRequest{
		Marker:           marker,
		AttributesToGet:  g.attributes,
		FilterExpression: g.filter,
	}
	reqBody, err := json.Marshal(getItemsReq)
	if err != nil {
		return nil, err
	}
	req.SetBody(reqBody)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	err = fasthttp.Do(req, resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 {
		body := string(resp.Body())
		if body != "" {
			body = ": \n" + body + "\n"
		}
		return nil, errors.Errorf("got unexpected response code %d from %s%s", resp.StatusCode(), g.targetUrl, body)
	}
	getItemsResp := &getItemsResponse{}
	err = json.Unmarshal(resp.Body(), getItemsResp)
	if err != nil {
		return nil, err
	}
	return getItemsResp, nil
}

// Same as RootCommandeer.buildUrl() but with a canonical, slash-terminated path appended.
func (g *GetItemsCommandeer) buildUrl() (*url.URL, error) {
	baseUrl, err := g.rootCommandeer.buildUrl()
	if err != nil {
		return nil, err
	}
	baseUrl.Path = path.Clean(baseUrl.Path+"/"+g.path) + "/"
	return baseUrl, nil
}
