package generator

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)

// ErrNotInDict indicates that value, which was requested, is not in the dictionary.
var ErrNotInDict = errors.New("tag not found in dictionary")

// ErrUseColName indicates that name of the column should be inserted as the content
var ErrUseColName = errors.New("use column name as string")

// Constant defining default values supported for the generation of the data.
const (
	TagName      = "@name"
	TagSurname   = "@surname"
	TagInteger   = "@integer"
	TagDecimal   = "@decimal"
	TagStreet    = "@street"
	TagCity      = "@city"
	TagState     = "@state"
	TagCountry   = "@country"
	TagDate      = "@date"
	TagDateTime  = "@datetime"
	TagTimestamp = "@timestamp"
	TagYesNo     = "@yn"
	TagBool      = "@bool"
	TagColName   = "@colname"

	RegexPhone      = `^(\d{9}|\+\d{11})$`
	RegexEmail      = `^[a-z]{5,10}@[a-z]{5,10}\.(com|net|org)$`
	RegexPostalCode = `^(\d{2})-(\d{3})$`
	RegexPESEL      = `^(\d{11})$`
	RegexNIP        = `^(\d{10})$`
	RegexREGON      = `^(\d{9})$`
	RegexIBAN       = `^([a-zA-Z]{2}[0-9]{2}[a-zA-Z0-9]{4}[0-9]{7}([a-zA-Z0-9]?){0,16})$`
	RegexWord       = `^([A-Z][a-z]+)(-[A-Z][a-z]+)?$`
)

// ReadDump reads JSON file and serializes it into GOB file.
func ReadDump(in string, out string) error {
	cfgFile, err := os.Open(in)
	if err != nil {
		return fmt.Errorf("unable to open %s file: %s", in, err.Error())
	}
	defer cfgFile.Close()

	b, err := ioutil.ReadAll(cfgFile)
	if err != nil {
		return fmt.Errorf("unable to read contents of %s file: %s", in, err.Error())
	}

	d := Dictionary{}
	err = json.Unmarshal(b, &d)
	if err != nil {
		return fmt.Errorf("unable to unmarshall contents of %s file: %s", in, err.Error())
	}

	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	if err = enc.Encode(d); err != nil {
		return fmt.Errorf("unable to encode %s file: %s", in, err.Error())
	}

	f, err := os.Create(out)
	if err != nil {
		return fmt.Errorf("unable to create output file %s: %s", out, err.Error())
	}
	defer f.Close()

	if _, err := f.Write(buff.Bytes()); err != nil {
		return fmt.Errorf("unable to write output file %s: %s", out, err.Error())
	}

	return nil
}
