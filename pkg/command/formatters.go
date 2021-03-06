package command

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

type itemWritter interface {
	Write(item Item) error
}

type csvItemWritter struct {
	under  *csv.Writer
	header []string
}

type jsonItemWritter struct{}

func NewCsvItemWritter() *csvItemWritter {
	return &csvItemWritter{
		under: csv.NewWriter(os.Stdout),
	}
}

func (w *csvItemWritter) Write(item Item) error {
	var err error
	if w.header == nil {
		w.header = extractSortedAttrNames(item)
		err = w.under.Write(w.header)
		if err != nil {
			return err
		}
		w.under.Flush()
	}
	var records []string
	for _, attrName := range w.header {
		attr := item[attrName]
		var attrValue string
		for _, x := range attr {
			attrValue = x
			break
		}
		records = append(records, attrValue)
	}
	err = w.under.Write(records)
	if err != nil {
		return err
	}
	w.under.Flush()
	return nil
}

func extractSortedAttrNames(item Item) []string {
	var sortedAttrNames []string
	for attrName := range item {
		sortedAttrNames = append(sortedAttrNames, attrName)
	}
	sort.Strings(sortedAttrNames)
	return sortedAttrNames
}

type NumberFromString string

func (i NumberFromString) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprint(i)), nil
}

func (w *jsonItemWritter) Write(item Item) error {
	m := make(map[string]interface{}, len(item))
	for attrName, attr := range item {
		var attrValue interface{}
		for attrType, v := range attr {
			if attrType == "N" {
				attrValue = NumberFromString(v)
			} else {
				attrValue = v
			}
		}
		m[attrName] = attrValue
	}
	line, err := json.Marshal(m)
	if err != nil {
		return err
	}
	fmt.Println(string(line))
	return nil
}
