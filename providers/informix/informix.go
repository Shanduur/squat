/*
Package informix includes implementation of the Provider interface.
*/
package informix

import (
	"database/sql"
	// embed is used here for including describe.sql file during compilation.
	_ "embed"
	"fmt"

	// driver for the SQL
	_ "github.com/alexbrainman/odbc"
	"github.com/shanduur/squat/config"
	"github.com/shanduur/squat/providers"
)

type ifxConfig struct {
	ProviderName   string            `toml:"provider-name"`
	DataSourceName string            `toml:"odbc-data-source"`
	Formats        providers.Formats `toml:"formats"`
}

//go:embed describe.sql
var describeQuery string

// IfxProvider struct is the structure implementing Provider interface
type Provider struct {
	cfg ifxConfig
}

// New creates new Informix Provider
func New(configPath string) (Provider, error) {
	var ifx Provider

	err := ifx.Initialize(configPath)
	if err != nil {
		return ifx, fmt.Errorf("unable to initialize: %s", err.Error())
	}

	return ifx, nil
}

// Initialize reads config and returns the provider with configuration read
// from the config. By default it is called by New function, but can be used standalone.
func (ifx *Provider) Initialize(configPath string) (err error) {
	err = config.ReadTOML(&ifx.cfg, configPath)
	if err != nil {
		return fmt.Errorf("unable to read config: %s", err.Error())
	}

	return nil
}

// GetTableDescription retrieves basic table description from database.
// Using describe.sql it retrieves info about every column of table.
func (ifx Provider) GetTableDescription(name string) (dsc []providers.Describe, err error) {
	conn, err := connect(ifx)
	if err != nil {
		return nil, fmt.Errorf("unable to connect: %s", err.Error())
	}

	stmt, err := conn.Prepare(describeQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %s", err.Error())
	}

	rows, err := stmt.Query(name)
	if err != nil {
		return nil, fmt.Errorf("unable to execute statement: %s", err.Error())
	}
	defer rows.Close()

	d := providers.Describe{}
	for rows.Next() {
		err = rows.Scan(&d.ColumnName, &d.ColumnType, &d.ColumnLength, &d.ColumnPrecision, &d.Nullable)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %s", err.Error())
		}
		dsc = append(dsc, d)
	}

	if len(dsc) < 1 {
		err = providers.ErrNoResult
	}

	return
}

// ProviderName is interface function.
func (ifx Provider) ProviderName() string {
	return ifx.cfg.ProviderName
}

// DateFormat is interface function.
func (ifx Provider) DateFormat() string {
	return ifx.cfg.Formats.DateFormat
}

// DateTimeFormat is interface function.
func (ifx Provider) DateTimeFormat() string {
	return ifx.cfg.Formats.DateTimeFormat
}

func connect(ifx Provider) (conn *sql.DB, err error) {
	conn, err = sql.Open("odbc", fmt.Sprintf("DSN=%s", ifx.cfg.DataSourceName))
	if err != nil {
		return nil, fmt.Errorf("openning connection failed: %s", err.Error())
	}

	return
}
