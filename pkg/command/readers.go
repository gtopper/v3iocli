package command

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"github.com/pkg/errors"
	"io"
)

type itemReader interface {
	read() ([]byte, error)
}

type jsonReader struct {
	under *bufio.Reader
	eof   bool
}

func (i *jsonReader) read() ([]byte, error) {
	if i.eof {
		return nil, io.EOF
	}
	str, err := i.under.ReadString('\n')
	if err != io.EOF {
		i.eof = true
	} else if err != nil {
		return nil, err
	}
	return []byte(str), err
}

type csvReader struct {
	under  *csv.Reader
	header []string
	key    string
}

func newCsvReader(reader io.Reader, key string) (*csvReader, error) {
	underInstance := csv.NewReader(reader)
	header, err := underInstance.Read()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read header")
	}
	return &csvReader{
		under:  underInstance,
		header: header,
		key:    key,
	}, nil
}

func (c *csvReader) read() ([]byte, error) {
	record, err := c.under.Read()
	if err != nil {
		return nil, err
	}
	var min int
	if len(c.header) < len(record) {
		min = len(c.header)
	} else {
		min = len(record)
	}
	var itemKey = make(map[string]map[string]string, 1)
	var item = make(map[string]map[string]string, min)
	i := 0
	for i < min {
		if c.header[i] == c.key {
			itemKey[c.header[i]] = map[string]string{"S": record[i]}
		} else {
			item[c.header[i]] = map[string]string{"S": record[i]}
		}
		i++
	}
	resultMap := map[string]interface{}{"Key": itemKey, "Item": item}
	result, err := json.Marshal(resultMap)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse JSON")
	}
	return result, nil
}
