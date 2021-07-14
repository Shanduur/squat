/*
Package postgres includes implementation of Provider interface, as well as all necessary helper functions.
*/
package postgres

import (

	// embed is used here for including describe.sql file during compilation.
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/shanduur/squat/config"
	"github.com/shanduur/squat/providers"
)

type pgConfig struct {
	ProviderName string            `toml:"provider-name"`
	Address      string            `toml:"address"`
	Port         uint16            `toml:"port"`
	Database     string            `toml:"database"`
	User         string            `toml:"user"`
	Password     string            `toml:"password"`
	Formats      providers.Formats `toml:"formats"`
}

//go:embed describe.sql
var describeQuery string

var dbURL = "postgresql://%v:%v/%v?user=%v&password=%v"

// Provider struct contains config and is the implementation of Providers
// interface for PostgreSQL database.
type Provider struct {
	cfg pgConfig
}

// New reads provided config and creates new Postgres Provider.
func New(configPath string) (Provider, error) {
	var p Provider

	err := p.Initialize(configPath)
	if err != nil {
		return p, fmt.Errorf("unable to initialize: %s", err.Error())
	}

	return p, nil
}

// Initialize reads config and returns the provider with configuration read
// from the config. By default it is called by New function, but can be used standalone.
func (p *Provider) Initialize(configPath string) (err error) {
	err = config.ReadTOML(&p.cfg, configPath)
	if err != nil {
		return fmt.Errorf("unable to read config: %s", err.Error())
	}

	return nil
}

// ProviderName is interface function.
func (p Provider) ProviderName() string {
	return p.cfg.ProviderName
}

// GetTableDescription retrieves basic table description from database.
// Using describe.sql it retrieves info about every column of table.
func (p Provider) GetTableDescription(name string) (dsc []providers.Describe, err error) {
	conn, err := connect(p)
	if err != nil {
		return nil, fmt.Errorf("unable to connect: %s", err.Error())
	}

	rows, err := conn.Query(context.Background(), describeQuery, name)
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

// DateFormat is interface function.
func (p Provider) DateFormat() string {
	return p.cfg.Formats.DateFormat
}

// DateTimeFormat is interface function.
func (p Provider) DateTimeFormat() string {
	return p.cfg.Formats.DateTimeFormat
}

func connect(p Provider) (conn *pgx.Conn, err error) {
	conn, err = pgx.Connect(context.Background(),
		fmt.Sprintf("postgresql://%s:%d/%s?user=%s&password=%s", p.cfg.Address, p.cfg.Port, p.cfg.Database, p.cfg.User, p.cfg.Password))
	if err != nil {
		err = fmt.Errorf("unable to connect to database: %s", err.Error())
		return
	}

	return
}
