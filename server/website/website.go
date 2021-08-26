/*
Package website holds strings with template contents and exports all functions
necessary to build the website contents.
*/
package website

import (
	// embed is used hre for including templates into the binary
	_ "embed"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/shanduur/squat/generator"
	"github.com/shanduur/squat/providers"
)

var (
	//go:embed templates/main.html
	mainHTML string
	//go:embed templates/.Head.html
	headHTML string
	//go:embed templates/.Body.index.html
	bodyIndexHTML string
	//go:embed templates/.Body.error.html
	bodyErrorHTML string
	//go:embed templates/.Body.table.html
	bodyTableHTML string
	//go:embed templates/.Script.table.js
	scriptTableJS string
	//go:embed templates/partials/.Option.html
	partialOptionHTML string
	//go:embed templates/partials/.Rows.html
	partialRowsHTML string
	//go:embed templates/partials/.Sources.html
	partialSourcesHTML string

	// template map is map of all tags used in template HTML files.
	template = map[string]string{
		"head":      "{{ .Head }}",
		"body":      "{{ .Body }}",
		"rows":      "{{ .Rows }}",
		"name":      "{{ .Name }}",
		"type":      "{{ .Type }}",
		"order":     "{{ .Order }}",
		"table":     "{{ .Table }}",
		"script":    "{{ .Script }}",
		"fomrat":    "{{ .Format }}",
		"length":    "{{ .Length }}",
		"source":    "{{ .Source }}",
		"options":   "{{ .Options }}",
		"sources":   "{{ .Sources }}",
		"display":   "{{ .Display }}",
		"message":   "{{ .Message }}",
		"template":  "{{ .Template }}",
		"precision": "{{ .Precision }}",
	}
)

// BuildTables is a function used for building document which contains form
// that is used to create GET request for the generator API.
func BuildTables(src string, tab string, dsc []providers.Describe) (string, error) {
	output := strings.ReplaceAll(mainHTML, template["head"], headHTML)
	output = strings.ReplaceAll(output, template["body"], bodyTableHTML)
	output = strings.ReplaceAll(output, template["script"], scriptTableJS)

	var options string
	g, err := generator.New(path.Join(os.Getenv("DATA_LOCATION"), "data.gob"))
	if err != nil {
		return "", fmt.Errorf("unable to load generator: %s", err.Error())
	}

	for k, v := range g.TagsAndRegex {
		opt := strings.ReplaceAll(partialOptionHTML, template["name"], fmt.Sprint(v))
		opt = strings.ReplaceAll(opt, template["display"], k)
		options = fmt.Sprintf("%s\n%s", options, opt)
	}

	output = strings.ReplaceAll(output, template["template"], options)

	var rows string
	for i, d := range dsc {
		opt := options
		row := partialRowsHTML
		row = strings.ReplaceAll(row, template["order"], fmt.Sprint(i))

		if d.ColumnType.Valid {
			row = strings.ReplaceAll(row, template["type"], d.ColumnType.String)
		} else {
			row = strings.ReplaceAll(row, template["type"], "")
		}

		if d.ColumnLength.Valid {
			row = strings.ReplaceAll(row, template["length"], fmt.Sprintf("%d", d.ColumnLength.Int64))
		} else {
			row = strings.ReplaceAll(row, template["length"], "1")
		}

		if d.ColumnPrecision.Valid {
			row = strings.ReplaceAll(row, template["precision"], fmt.Sprintf("%d", d.ColumnPrecision.Int64))
		} else {
			row = strings.ReplaceAll(row, template["precision"], "0")
		}

		if strings.Contains(strings.ToUpper(d.ColumnType.String), generator.TypeInt) {
			opt = strings.ReplaceAll(opt, generator.TagInteger+`"`, generator.TagInteger+`"`+`selected="selected"`)

		} else if strings.Contains(strings.ToUpper(d.ColumnType.String), generator.TypeChar) {
			opt = strings.ReplaceAll(opt, generator.RegexWord+`"`, generator.RegexWord+`"`+`selected="selected"`)

		} else if strings.Contains(strings.ToUpper(d.ColumnType.String), generator.TypeFloat) {
			opt = strings.ReplaceAll(opt, generator.TagDecimal+`"`, generator.TagDecimal+`"`+`selected="selected"`)

		} else if strings.Contains(strings.ToUpper(d.ColumnType.String), generator.TypeDecimal) {
			opt = strings.ReplaceAll(opt, generator.TagDecimal+`"`, generator.TagDecimal+`"`+`selected="selected"`)

		} else if strings.Contains(strings.ToUpper(d.ColumnType.String), generator.TypeTimestamp) {
			opt = strings.ReplaceAll(opt, generator.TagTimestamp+`"`, generator.TagTimestamp+`"`+`selected="selected"`)

		} else if strings.Contains(strings.ToUpper(d.ColumnType.String), generator.TypeDateTime) {
			opt = strings.ReplaceAll(opt, generator.TagDateTime+`"`, generator.TagDateTime+`"`+`selected="selected"`)

		} else if strings.Contains(strings.ToUpper(d.ColumnType.String), generator.TypeDate) {
			opt = strings.ReplaceAll(opt, generator.TagDate+`"`, generator.TagDate+`"`+`selected="selected"`)

		}

		if d.ColumnName.Valid {
			row = strings.ReplaceAll(row, template["name"], d.ColumnName.String)
		}

		if d.Nullable.Valid {
			if d.Nullable.Bool {
				row = strings.ReplaceAll(row, `name="nullable`, `checked=true name="nullable`)
			}
		}

		row = strings.ReplaceAll(row, template["options"], opt)

		rows = fmt.Sprintf("%s\n%s", rows, row)
	}

	output = strings.ReplaceAll(output, template["rows"], rows)
	output = strings.ReplaceAll(output, template["source"], src)
	output = strings.ReplaceAll(output, template["table"], tab)

	return output, nil
}

// BuildIndex is the function for building HTML document consisting of
// standard HTML parts. It builds document that is displayed as index.
func BuildIndex(providers map[string]providers.Provider) string {
	output := strings.ReplaceAll(mainHTML, template["head"], headHTML)
	output = strings.ReplaceAll(output, template["body"], bodyIndexHTML)
	output = strings.ReplaceAll(output, template["script"], "")
	output = strings.ReplaceAll(output, template["sources"], partialSourcesHTML)

	var options string
	for i := range providers {
		opt := strings.ReplaceAll(partialOptionHTML, template["name"], i)
		opt = strings.ReplaceAll(opt, template["display"], i)
		options = fmt.Sprintf("%s\n%s", options, opt)
	}

	output = strings.ReplaceAll(output, template["options"], options)

	return output
}
